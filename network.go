package chord

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

func CreateNewNode(address string, joinNodeAddr string) (*RPCNode, error) {
	// Initially do not skip deferred functions
	// deferred functions are to be skipped in
	// case of errors
	skipDefer := false

	id := getHash(address)

	// Discards logger warnings regarding Save and Stop
	// Non RPC methods not having signature required as per
	// net/rpc package
	log.SetOutput(io.Discard)

	// Initialize RPC node
	node := &RPCNode{
		Node: &Node{
			id:              id,
			address:         address,
			predecessorId:   nil,
			predecessorRPC:  nil,
			predecessorAddr: "",
			store:           make(dataStore),
			exitCh:          make(chan struct{}),
		},
	}

	// Initialize connection to database
	node.db, _ = sql.Open("sqlite3", "../connections.db")

	// start rpc server for node and listen
	// for connections
	rpc.Register(node)
	rpc.HandleHTTP()

	var err error
	node.listener, err = net.Listen("tcp", address)
	if err != nil {
		skipDefer = true
		return nil, ErrUnableToListen
	}
	go http.Serve(node.listener, nil)

	// create rpc client for node and save it
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		skipDefer = true
		return nil, ErrUnableToDial
	}
	node.self = client

	// populate finger table.
	// successor of node is the node itself initially,
	// and is not updated if there aren't any other
	// nodes in the network i.e. joinNodeAddr was empty
	node.fingerTable = make([]*Finger, 30)
	node.fingerTable[0] = &Finger{node.id, node.address}

	go saveNode(node.db, node.address, node.fingerTable[0].address)

	// prediodically checks if predecessor has failed
	defer func() {
		if skipDefer {
			fmt.Println("Skipping predecessor checks")
			return
		}
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			for {
				select {
				case <-ticker.C:
					node.checkPredecessor()
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
			fmt.Println("Skipping finger fixes")
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
			fmt.Println("Skipping stabilize")
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

	// empty join address implies creation of
	// new network, hence return the new node
	if joinNodeAddr == "" {
		fmt.Printf("============ New Network ============\n\n")
		fmt.Printf("Node: %v\nNode ID: %v\n",
			node.address,
			toBigInt(node.id),
		)
		return node, nil
	}

	// Non empty joinNodeAddr implies
	// this node has to join exitsting network

	joinNodeClient, err := getClient(joinNodeAddr)
	if err != nil {
		skipDefer = true
		return nil, ErrUnableToDial
	}

	// find appropriate successor of new node
	var successorAddr string
	joinNodeClient.Call("RPCNode.Successor", node.id, &successorAddr)
	joinNodeClient.Close()

	successorRPC, _ := getClient(successorAddr)
	var successorId []byte
	successorRPC.Call("RPCNode.GetId", "", &successorId)

	if equal(successorId, node.id) {
		// Node with same ID already exists in the
		// network
		successorRPC.Close()
		return nil, ErrNodeAlreadyExists
	}

	// update first finger to point to successor
	node.fingerTable[0].id = successorId
	node.fingerTable[0].address = successorAddr

	// update db
	go updateSuccessor(node.db, node.address, successorAddr)

	// notify successor that new node might
	// be its new predecessor
	successorRPC.Call("RPCNode.Notify", node.address, "")
	successorRPC.Close()

	fmt.Printf("============ Joining Node ============\n\n")
	fmt.Printf("Node: %v\nNode ID: %v\n",
		node.address,
		toBigInt(node.id),
	)
	return node, nil
}
