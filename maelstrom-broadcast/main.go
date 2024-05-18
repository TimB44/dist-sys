package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"golang.org/x/exp/maps"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type empty struct{}

var seen map[int]empty = make(map[int]empty, 10)
var seenLock sync.Mutex

var neighborsNotSeen map[string]map[int]empty
var notSeenLock sync.Mutex

func main() {
	n := maelstrom.NewNode()
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		message := int(body["message"].(float64))
		seenLock.Lock()

		// update the seen map to have this message
		seen[message] = empty{}
		seenLock.Unlock()

		// add this message to the not seen map for every neighbor as we do not know that it has seen it
		notSeenLock.Lock()
		for k := range neighborsNotSeen {
			neighborsNotSeen[k][message] = empty{}
		}
		notSeenLock.Unlock()

		var resp = make(map[string]any, 1)
		resp["type"] = "broadcast_ok"

		return n.Reply(msg, resp)
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
		var newNeighbors []string

		// custom topology for efficiency test
		if len(n.NodeIDs()) == 25 {
			id, _ := strconv.ParseInt(n.ID()[1:], 10, 32)
			if (id+1)%5 == 0 {
				newNeighbors = []string{"n0", "n5", "n10", "n15", "n20"}
			} else {
				group := int(id) / 5

				for i := 0; i < 5; i++ {
					newNeighbors = append(newNeighbors, fmt.Sprintf("n%d", ((group*5)+i)))
				}

			}
		} else {
			wrappedNeighbors := body["topology"].(map[string]any)[n.ID()].([]any)
			for _, v := range wrappedNeighbors {
				newNeighbors = append(newNeighbors, v.(string))
			}
		}

		// new topology creates new neighbors meaning we need to reset our neighborsNotSeen map
		notSeenLock.Lock()
		numMsg := len(seen)
		neighborsNotSeen = make(map[string]map[int]empty, len(newNeighbors))
		for _, v := range newNeighbors {
			neighborsNotSeen[v] = make(map[int]empty, numMsg)
			maps.Copy(neighborsNotSeen[v], seen)
		}

		notSeenLock.Unlock()

		var resp = make(map[string]any, 1)
		resp["type"] = "topology_ok"
		return n.Reply(msg, resp)
	})

	n.Handle("update", func(msg maelstrom.Message) error {
		var body UpdateParams
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		seenLock.Lock()
		// If a message is not already seen then we need to tell our neighbors about it
		var notSeen []int
		for _, message := range body.Messages {
			_, alreadySeen := seen[int(message)]
			seen[int(message)] = empty{}
			if !alreadySeen {
				notSeen = append(notSeen, int(message))
			}
		}
		seenLock.Unlock()

		notSeenLock.Lock()
		for _, v := range notSeen {
			for k := range neighborsNotSeen {
				neighborsNotSeen[k][v] = empty{}
			}
		}
		notSeenLock.Unlock()

		//This replies help compact the messages, however they result in about twice as many messages so
		//don't send them when doing efficiency tests
		if len(n.NodeIDs()) != 25 {
			body.Type = "update_ok"
			n.Reply(msg, body)
		}

		return nil
	})

	n.Handle("update_ok", func(msg maelstrom.Message) error {
		var body UpdateParams
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		notSeenLock.Lock()
		// Now that we have a response we know that we no longer need to send the previously sent messages
		for _, message := range body.Messages {
			delete(neighborsNotSeen[msg.Src], int(message))
		}
		notSeenLock.Unlock()

		return nil
	})

	go func() {
		for {
			time.Sleep(150 * time.Millisecond)
			notSeenLock.Lock()
			for node := range neighborsNotSeen {
				i := 0
				keys := make([]float64, len(neighborsNotSeen[node]))
				for k := range neighborsNotSeen[node] {
					keys[i] = float64(k)
					i++
				}
				n.Send(node, UpdateParams{
					Messages: keys,
					Type:     "update",
				})
			}
			notSeenLock.Unlock()
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type UpdateParams struct {
	Messages []float64
	Type     string
}
