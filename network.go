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
	client, err := rpc.DialHTTP("tpc", address)
	if err != nil {
		return nil, ErrUnableToDial
	}
	node.self = client

	// populate finger table
	// successor of node is node itself if there
	// aren't any other nodes in the network
	node.fingerTable = append(node.fingerTable, &Finger{node.id, node.self})

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

	return node, nil
}
