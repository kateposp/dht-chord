package chord

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

func CreateNewNode(address string, joinNodeAddr string) (*Node, error) {
	skipDefer := false

	id := getHash(address)

	// Initialize node
	node := &Node{
		id:              id,
		address:         address,
		predecessorId:   nil,
		predecessorRPC:  nil,
		predecessorAddr: "",
		store:           make(dataStore),
		exitCh:          make(chan struct{}),
	}

	// start rpc server for node
	rpc.Register(node)
	rpc.HandleHTTP()

	var err error
	node.listener, err = net.Listen("tcp", address)
	if err != nil {
		skipDefer = true
		return nil, ErrUnableToListen
	}
	go http.Serve(node.listener, nil)

	// create rpc client for node
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		skipDefer = true
		return nil, ErrUnableToDial
	}
	node.self = client

	// populate finger table
	// successor of node is node itself if there
	// aren't any other nodes in the network
	node.fingerTable = make([]*Finger, 30)
	node.fingerTable[0] = &Finger{node.id, &node.address}

	// prediodically check if predecessor has failed
	defer func() {
		if skipDefer {
			log.Println("Skipping predecessor checks")
			return
		}
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			for {
				select {
				case <-ticker.C:
					err := node.checkPredecessor()
					if err != nil {
						fmt.Println(err)
					}
				case <-node.exitCh:
					ticker.Stop()
					return
				}
			}
		}()
	}()

	// prediodically fix finger table
	defer func() {
		if skipDefer {
			log.Println("Skipping finger fixes")
			return
		}
		go func() {
			fingerIndex := 0
			ticker := time.NewTicker(100 * time.Millisecond)
			for {
				select {
				case <-ticker.C:
					if fingerIndex >= 30 {
						fingerIndex = 0
					}
					fingerIndex = node.fixFinger(fingerIndex)
				case <-node.exitCh:
					ticker.Stop()
					return
				}
			}
		}()
	}()

	// prediodically stablize the node
	defer func() {
		if skipDefer {
			log.Println("Skipping stabilize")
			return
		}
		go func() {
			ticker := time.NewTicker(2 * time.Second)
			for {
				select {
				case <-ticker.C:
					node.stabilize()
				case <-node.exitCh:
					ticker.Stop()
					return
				}
			}
		}()
	}()

	// empty join address means new network
	// hence return the new node
	if joinNodeAddr == "" {
		return node, nil
	}

	// join address was not empty means
	// this node has to join exitsting network

	joinNodeClient, err := rpc.DialHTTP("tcp", joinNodeAddr)
	if err != nil {
		skipDefer = true
		return nil, ErrUnableToDial
	}

	// find appropriate successor of new node
	var successorAddr string
	joinNodeClient.Call("Node.Successor", node.id, &successorAddr)

	successorRPC, _ := getClient(&successorAddr)
	var successorId []byte
	successorRPC.Call("Node.GetId", "", &successorId)
	defer successorRPC.Close()

	if equal(successorId, node.id) {
		return nil, ErrNodeAlreadyExists
	}

	// update first finger to point to successor
	node.fingerTable[0].id = successorId
	node.fingerTable[0].address = &successorAddr

	// notify successor that new node might
	// be its predecessor
	successorRPC.Call("Node.Notify", &node, "")

	// get appropriate data from successor
	successorRPC.Call("Node.TransferData", &node.address, "")
	return node, nil
}
