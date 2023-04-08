package main

import (
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map.
		var requestBody map[string]any
		if err := json.Unmarshal(msg.Body, &requestBody); err != nil {
			return err
		}

		responseBody := make(map[string]any)
		responseBody["type"] = "generate_ok"
		responseBody["in_reply_to"] = requestBody["msg_id"]
		responseBody["id"] = fmt.Sprintf("%s-%d", n.ID(), requestBody["msg_id"])

		return n.Reply(msg, responseBody)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Fatalf("ERROR: %s", err)
	}
}
