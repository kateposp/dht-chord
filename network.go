package chord

import (
	"crypto/sha1"
	"net"
	"net/http"
	"net/rpc"
)

func createNewNode(address string, joinNodeAddr string) (*Node, error) {
	// create []byte from address using sha1
	h := sha1.New()
	h.Write([]byte(address))

	// Initialize node
	node := &Node{
		id:             h.Sum(nil),
		address:        address,
		predecessorId:  nil,
		predecessorRPC: nil,
	}

	// start rpc server for node
	rpc.Register(node)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", address)
	if err != nil {
		return nil, ErrUnableToListen
	}
	go http.Serve(l, nil)

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
