package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type empty struct{}

var seen map[int]empty = make(map[int]empty, 10)
var seenLock sync.Mutex
var neighbors []string
var neighborsLock sync.Mutex

func main() {
	n := maelstrom.NewNode()
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		message := int(body["message"].(float64))
		seenLock.Lock()

		_, alreadySeen := seen[message]
		seen[message] = empty{}

		if !alreadySeen {

			for _, node := range neighbors {
				for m := range seen {
					n.Send(node, map[string]any{
						"type":    "broadcast",
						"message": m,
					})
				}

			}
		}
		seenLock.Unlock()

		var resp = make(map[string]any, 1)
		resp["type"] = "broadcast_ok"

		return n.Reply(msg, resp)
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {

		i := 0
		seenLock.Lock()
		keys := make([]int, len(seen))
		for k := range seen {
			keys[i] = k
			i++
		}
		seenLock.Unlock()

		var body = make(map[string]any, 1)
		body["type"] = "read_ok"
		body["messages"] = keys

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		neighborsLock.Lock()
		var newNeighbors []string
		wrappedNeighbors := body["topology"].(map[string]any)[n.ID()].([]any)
		for _, v := range wrappedNeighbors {
			newNeighbors = append(newNeighbors, v.(string))
		}
		neighbors = newNeighbors
		neighborsLock.Unlock()

		var resp = make(map[string]any, 1)
		resp["type"] = "topology_ok"
		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
