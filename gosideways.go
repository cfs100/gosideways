package gosideways

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Node struct {
	Addr     string
	Port     int
	Data     map[string]Data
	Siblings map[string]*Node

	save chan Data
	repl chan Data
}

type Data struct {
	Key     string    `json:"key"`
	Text    string    `json:"text"`
	Date    time.Time `json:"date"`
	Expires time.Time `json:"expires"`
}

func (n *Node) AddSibling(addr string, port int) error {
	key := addr + ":" + fmt.Sprint(port)
	if _, exists := n.Siblings[key]; exists {
		return errors.New("Sibling " + key + " already registered")
	}

	n.Siblings[key] = newNode(addr, port)

	return nil
}

func Listen(port int) *Node {
	node := newNode("127.0.0.1", port)
	go node.clean()
	go node.run()

	go func() {
		for d := range node.save {
			node.Data[d.Key] = d
		}
	}()

	go func() {
		for d := range node.repl {
			node.replicate(d)
		}
	}()

	node.load()

	return node
}

func (n *Node) run() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", n.Port))
	if err != nil {
		log.Println(err.Error())
		return
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(err.Error())
			continue
		}

		go n.handleConnection(conn)
	}
}

func (n *Node) Get(key string) *Data {
	return n.get(key)
}

func (n *Node) Set(key string, data string, valid time.Duration) {
	d := n.newData(key, data, valid)

	n.save <- d
	n.repl <- d
}

func (n *Node) newData(key string, data string, valid time.Duration) Data {
	return Data{
		Key:     key,
		Text:    data,
		Date:    time.Now(),
		Expires: time.Now().Add(valid),
	}
}

func (n *Node) replicate(d Data) {
	for _, s := range n.Siblings {
		if s.Addr == n.Addr && s.Port == n.Port {
			continue
		}

		addr := fmt.Sprintf("%s:%d", s.Addr, s.Port)
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			log.Println(err.Error())
			continue
		}

		exp := int(d.Expires.Sub(time.Now()).Seconds() + .5)

		fmt.Fprintf(conn, "REPLICATE %s %d %d %s", d.Key, exp, len(d.Text), d.Text)
		conn.Close()
	}
}

func (n *Node) get(key string) *Data {
	var data *Data

	if aux, ok := n.Data[key]; ok {
		if aux.Expires.Before(time.Now()) {
			n.del(key)
		} else {
			data = &aux
		}
	}

	return data
}

func (n *Node) del(key string) {
	if _, ok := n.Data[key]; ok {
		delete(n.Data, key)
	}
}

func (n *Node) clean() {
	const seconds = 5
	for {
		now := time.Now()
		for key, data := range n.Data {
			if data.Expires.Before(now) {
				n.del(key)
			}
		}

		if now.Minute()%seconds == 0 && now.Second() < seconds {
			n.dump()
		}

		time.Sleep(seconds * time.Second)
	}
}

func (n Node) dump() error {
	data, err := json.Marshal(n.Data)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(n.getFilename(), data, 0600)
	if err != nil {
		return err
	}

	return nil
}

func (n *Node) load() {
	data, _ := ioutil.ReadFile(n.getFilename())
	if len(data) <= 2 {
		return
	}

	json.Unmarshal(data, &n.Data)
}

func (n Node) getFilename() string {
	return fmt.Sprintf("%s/gosideways-%s-%d.json", os.TempDir(), n.Addr, n.Port)
}

func newNode(addr string, port int) *Node {
	return &Node{
		Addr:     addr,
		Port:     port,
		Data:     make(map[string]Data),
		Siblings: make(map[string]*Node),

		save: make(chan Data, 1024),
		repl: make(chan Data, 1024),
	}
}

func (n *Node) handleConnection(c net.Conn) {
	defer c.Close()

	scanner := bufio.NewScanner(c)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		command := strings.SplitN(line, " ", 3)
		if len(command) < 2 {
			continue
		}

		command[0] = strings.ToUpper(command[0])
		for i := 1; i < len(command); i++ {
			command[i] = strings.TrimSpace(command[i])
		}

		switch command[0] {
		case "GET":
			data := n.get(command[1])
			if data != nil {
				fmt.Fprintln(c, data.Text)
			}
			break
		case "SET", "REPLICATE":
			if len(command) < 3 {
				continue
			}

			aux := strings.SplitN(command[2], " ", 3)
			if len(aux) < 3 {
				continue
			}

			seconds, _ := strconv.Atoi(aux[0])
			if seconds <= 0 {
				continue
			}

			length, _ := strconv.Atoi(aux[1])
			if length <= 0 {
				continue
			}

			d := n.newData(command[1], aux[2], time.Duration(seconds)*time.Second)

			for len(d.Text) < length {
				scanner.Scan()
				d.Text += "\n" + scanner.Text()
			}

			n.save <- d

			if command[0] == "SET" {
				n.repl <- d
			}
			break
		case "DELETE":
			n.del(command[1])
			break
		}
	}
}
