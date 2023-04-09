package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type (
	BroadcastMsg struct {
		Type  string `json:"type"`
		Value int    `json:"message"`
	}

	TopologyMsg struct {
		Topology map[string][]string `json:"topology"`
	}

	server struct {
		node   *maelstrom.Node
		nodeId string

		messages             map[int]struct{}
		messagesInserterChan chan int
		broadcastChans       map[string]chan int
	}
)

func NewBroadcastMsg(value int) BroadcastMsg {
	return BroadcastMsg{
		Type:  "broadcast",
		Value: value,
	}
}

func main() {
	n := maelstrom.NewNode()

	server := server{
		node:                 n,
		nodeId:               n.ID(),
		messages:             make(map[int]struct{}),
		messagesInserterChan: make(chan int),
		broadcastChans:       make(map[string]chan int),
	}
	go insertMessages(server.messages, server.messagesInserterChan)

	n.Handle("broadcast", server.HandleBroadcastMsg)
	n.Handle("read", server.HandleReadMsg)
	n.Handle("topology", server.HandleTopologyMsg)

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Fatalf("ERROR: %s", err)
	}
}

func insertMessages(messages map[int]struct{}, inserterChan <-chan int) {
	for {
		select {
		case val, ok := <-inserterChan:
			if ok {
				messages[val] = struct{}{}
			} else {
				break
			}
		}
	}
}

func (s server) HandleBroadcastMsg(msg maelstrom.Message) error {
	var requestBody BroadcastMsg
	if err := json.Unmarshal(msg.Body, &requestBody); err != nil {
		return err
	}

	// Ignore if we've already received this broadcast
	_, ok := s.messages[requestBody.Value]
	if ok {
		return nil
	}
	s.messagesInserterChan <- requestBody.Value

	// Broadcast to other nodes in cluster
	for node, c := range s.broadcastChans {
		if node == msg.Src {
			continue
		}

		c <- requestBody.Value
	}

	responseBody := make(map[string]any)
	responseBody["type"] = "broadcast_ok"
	return s.node.Reply(msg, responseBody)
}

func (s server) HandleReadMsg(msg maelstrom.Message) error {
	responseBody := make(map[string]any)
	responseBody["type"] = "read_ok"

	messages := []int{}
	for m, _ := range s.messages {
		messages = append(messages, m)
	}

	responseBody["messages"] = messages
	return s.node.Reply(msg, responseBody)
}

func (s server) HandleTopologyMsg(msg maelstrom.Message) error {
	var tm TopologyMsg
	if err := json.Unmarshal(msg.Body, &tm); err != nil {
		return err
	}

	for node, _ := range tm.Topology {
		if node == s.nodeId {
			continue
		}

		c := make(chan int, 100)
		// Need to fill channel with existing state? If so: needs mutex?

		s.broadcastChans[node] = c
		go broadcastToNode(s.node, node, c)
	}

	responseBody := make(map[string]any)
	responseBody["type"] = "topology_ok"
	return s.node.Reply(msg, responseBody)
}

func broadcastToNode(sender *maelstrom.Node, dest string, messages <-chan int) {
	for {
		select {
		case val, ok := <-messages:
			if ok {
				sender.SyncRPC(context.Background(), dest, NewBroadcastMsg(val))
			}
		}
	}
}
