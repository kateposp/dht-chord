package chord

import (
	"crypto/sha1"
	"net"
	"net/http"
	"net/rpc"
)

func createNewNode(address string, joinNodeAddr string) (*Node, error) {
	h := sha1.New()
	h.Write([]byte(address))

	node := &Node{
		id:             h.Sum(nil),
		address:        address,
		predecessorId:  nil,
		predecessorRPC: nil,
	}

	rpc.Register(node)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", address)
	if err != nil {
		return nil, ErrUnableToListen
	}

	go http.Serve(l, nil)

	client, err := rpc.DialHTTP("tpc", address)
	if err != nil {
		return nil, ErrUnableToDial
	}

	node.self = client

	node.fingerTable = append(node.fingerTable, &Finger{node.id, node.self})

	if joinNodeAddr == "" {
		return node, nil
	}

	return nil, nil

	return node, nil
}
