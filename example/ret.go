package main

import (
	"fmt"
	"net/rpc"
)

func main() {
	client, err := rpc.DialHTTP("tcp", "localhost:12336")
	if err != nil {
		fmt.Println(err)
	}
	key := "8"
	var value string

	client.Call("RPCNode.Retrieve", &key, &value)
	fmt.Printf("%q %q\n", key, value)
}
