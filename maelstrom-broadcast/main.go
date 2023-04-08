package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type (
	Broadcast struct {
		Message int `json:"message"`
	}
)

func main() {
	n := maelstrom.NewNode()

	messages := []int{}
	messagesChan := make(chan int)
	go func() {
		for {
			select {
			case val, ok := <-messagesChan:
				if ok {
					messages = append(messages, val)
				} else {
					break
				}
			}
		}

	}()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var requestBody Broadcast
		if err := json.Unmarshal(msg.Body, &requestBody); err != nil {
			return err
		}
		messagesChan <- requestBody.Message

		responseBody := make(map[string]any)
		responseBody["type"] = "broadcast_ok"
		return n.Reply(msg, responseBody)
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		responseBody := make(map[string]any)
		responseBody["type"] = "read_ok"
		responseBody["messages"] = messages
		return n.Reply(msg, responseBody)
	})
	n.Handle("topology", func(msg maelstrom.Message) error {
		responseBody := make(map[string]any)
		responseBody["type"] = "topology_ok"
		return n.Reply(msg, responseBody)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Fatalf("ERROR: %s", err)
	}
}
