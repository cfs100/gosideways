package gosideways

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

type Node struct {
	Addr string
	Port int
	Data map[string]Data
}

type Data struct {
	Key     string
	Text    string
	Date    time.Time
	Expires time.Time
}

func Listen(port int) *Node {
	node := newNode("127.0.0.1", port)
	go node.clean()
	go node.Run()
	return node
}

func (n *Node) Run() {
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

func (n *Node) set(key string, data string, exp time.Duration) {
	n.Data[key] = Data{
		Key:     key,
		Text:    data,
		Date:    time.Now(),
		Expires: time.Now().Add(exp),
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
	for {
		now := time.Now()
		for key, data := range n.Data {
			if data.Expires.Before(now) {
				n.del(key)
			}
		}

		time.Sleep(time.Second)
	}
}

func newNode(addr string, port int) *Node {
	return &Node{
		Addr: addr,
		Port: port,
		Data: make(map[string]Data),
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
		case "SET":
			if len(command) < 3 {
				continue
			}

			aux := strings.SplitN(command[2], " ", 2)
			if len(aux) < 2 {
				continue
			}

			seconds, _ := strconv.Atoi(aux[0])
			if seconds <= 0 {
				continue
			}

			n.set(command[1], aux[1], time.Duration(seconds)*time.Second)
			break
		case "DELETE":
			n.del(command[1])
			break
		}
	}
}
