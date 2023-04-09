package main

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

type (
	server struct {
		node       *maelstrom.Node
		id         string
		messages   map[int]struct{}
		mutex      sync.Mutex
		neighbours map[string]*neighbour
	}

	neighbour struct {
		quit            chan bool
		nodeId          string
		unAckedMessages map[int]struct{}
		mutex           sync.Mutex
	}
)

func main() {
	f, err := os.OpenFile("/tmp/maelstrom.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(f)

	n := maelstrom.NewNode()

	server := server{
		node:       n,
		messages:   make(map[int]struct{}),
		neighbours: make(map[string]*neighbour),
	}

	n.Handle("init", server.HandleInitMsg)
	n.Handle("broadcast", server.HandleBroadcastMsg)
	n.Handle("read", server.HandleReadMsg)
	n.Handle("topology", server.HandleTopologyMsg)

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

func (s *server) HandleInitMsg(msg maelstrom.Message) error {
	s.id = s.node.ID()

	neighbourIds := make([]string, 0)
	for _, neighbourId := range s.node.NodeIDs() {
		// Don't broadcast to self
		if neighbourId == s.node.ID() {
			continue
		}
		neighbourIds = append(neighbourIds, neighbourId)
	}

	log.Printf("topology - node: %s // neighbours: %v", s.id, neighbourIds)
	for _, neighbourId := range neighbourIds {
		quit := make(chan bool)
		n := &neighbour{
			quit:            quit,
			nodeId:          neighbourId,
			unAckedMessages: make(map[int]struct{}),
		}
		s.neighbours[neighbourId] = n
		go broadcastToNeighbour(s.node, n)
	}

	return nil
}

func (s *server) HandleBroadcastMsg(msg maelstrom.Message) error {
	msgBody := make(map[string]any)
	if err := json.Unmarshal(msg.Body, &msgBody); err != nil {
		return err
	}

	var messages []int
	if m, ok := msgBody["message"]; ok {
		messages = append(messages, int(m.(float64)))
	}
	if m, ok := msgBody["messages"]; ok {
		for _, m := range m.([]any) {
			messages = append(messages, int(m.(float64)))
		}
	}

	s.mutex.Lock()
	log.Printf("broadcastRx - dst: %s // src: %s // msg: %v", s.id, msg.Src, messages)
	for _, m := range messages {
		_, ok := s.messages[m]
		if ok {
			continue
		}

		s.messages[m] = struct{}{}
	}
	s.mutex.Unlock()

	// Broadcast to other nodes in cluster
	for neighbourId, neighbour := range s.neighbours {
		// Don't send to node this message came from
		if neighbourId == msg.Src {
			continue
		}

		neighbour.mutex.Lock()
		for _, m := range messages {
			neighbour.unAckedMessages[m] = struct{}{}
		}
		neighbour.mutex.Unlock()
	}

	responseBody := make(map[string]string)
	responseBody["type"] = "broadcast_ok"
	return s.node.Reply(msg, responseBody)
}

func (s *server) HandleReadMsg(msg maelstrom.Message) error {
	responseBody := make(map[string]any)
	responseBody["type"] = "read_ok"

	s.mutex.Lock()
	messages := []int{}
	for m, _ := range s.messages {
		messages = append(messages, m)
	}
	s.mutex.Unlock()

	responseBody["messages"] = messages
	return s.node.Reply(msg, responseBody)
}

func (s *server) HandleTopologyMsg(msg maelstrom.Message) error {
	responseBody := make(map[string]any)
	responseBody["type"] = "topology_ok"
	return s.node.Reply(msg, responseBody)
}

func broadcastToNeighbour(sender *maelstrom.Node, n *neighbour) {
	send := func(messages []int) {
		ctxTimeout, cancel := context.WithTimeout(context.Background(), time.Second*1)
		defer cancel()

		outgoing := make(map[string]any)
		outgoing["type"] = "broadcast"
		outgoing["messages"] = messages

		log.Printf("broadcastTx - dst: %s // src: %s // msg: %v", sender.ID(), n.nodeId, messages)
		_, err := sender.SyncRPC(ctxTimeout, n.nodeId, outgoing)
		if err != nil {
			log.Errorf("Error neighbour SyncRPC: %s", err)
			return
		}

		n.mutex.Lock()
		for _, m := range messages {
			delete(n.unAckedMessages, m)
		}
		n.mutex.Unlock()
	}

	for {
		select {
		case <-n.quit:
			return
		default:
			n.mutex.Lock()
			if len(n.unAckedMessages) == 0 {
				n.mutex.Unlock()
				time.Sleep(time.Millisecond * 5)
				continue
			}

			messages := make([]int, 0, len(n.unAckedMessages))
			for m, _ := range n.unAckedMessages {
				messages = append(messages, m)
			}
			n.mutex.Unlock()
			go send(messages)

			time.Sleep(time.Duration(50) * time.Millisecond)
		}
	}
}
