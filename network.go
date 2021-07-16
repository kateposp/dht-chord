package chord

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

func createNewNode(address string, joinNodeAddr string) (*Node, error) {

	id := getHash(address)

	// Initialize node
	node := &Node{
		id:             id,
		address:        address,
		predecessorId:  nil,
		predecessorRPC: nil,
		exitCh:         make(chan struct{}),
	}

	// start rpc server for node
	rpc.Register(node)
	rpc.HandleHTTP()

	var err error
	node.listener, err = net.Listen("tcp", address)
	if err != nil {
		return nil, ErrUnableToListen
	}
	go http.Serve(node.listener, nil)

	// create rpc client for node
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return nil, ErrUnableToDial
	}
	node.self = client

	// populate finger table
	// successor of node is node itself if there
	// aren't any other nodes in the network
	node.fingerTable = append(node.fingerTable, &Finger{node.id, node.self})

	// prediodically check if predecessor has failed
	defer func() {
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			for {
				select {
				case <-ticker.C:
					err := node.checkPredecessor()
					if err != nil {
						fmt.Println("Predecessor has failed")
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
		go func() {
			fingerIndex := 0
			ticker := time.NewTicker(100 * time.Millisecond)
			for {
				select {
				case <-ticker.C:
					if fingerIndex > 30 {
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
		return nil, ErrUnableToDial
	}

	// find appropriate successor of new node
	var successor rpc.Client
	joinNodeClient.Call("Node.Successor", node.id, &successor)

	var successorId []byte
	successor.Call("Node.GetId", "", &successorId)

	if equal(successorId, node.id) {
		return nil, ErrNodeAlreadyExists
	}

	// update first finger to point to successor
	node.fingerTable[0].id = successorId
	node.fingerTable[0].node = &successor

	// notify successor that new node might
	// be its predecessor
	successor.Call("Node.Notify", &node, "")

	// get appropriate data from successor
	successor.Call("Node.TransferData", node.self, "")
	return node, nil
}
