package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type (
	Add struct {
		Delta int `json:"delta"`
	}
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		var add Add
		if err := json.Unmarshal(msg.Body, &add); err != nil {
			return err
		}

		counter, _ := kv.ReadInt(context.Background(), "counter")
		err := kv.CompareAndSwap(context.Background(), "counter", counter, counter+add.Delta, true)
		if err != nil {
			return err
		}

		responseBody := make(map[string]any)
		responseBody["type"] = "add_ok"
		return n.Reply(msg, responseBody)
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		counter, err := kv.ReadInt(context.Background(), "counter")
		if err != nil {
			return err
		}

		responseBody := make(map[string]any)
		responseBody["type"] = "read_ok"
		responseBody["value"] = counter
		return n.Reply(msg, responseBody)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Fatalf("ERROR: %s", err)
	}
}
