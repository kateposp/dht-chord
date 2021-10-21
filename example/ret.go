package main

import (
	"fmt"
	"net/rpc"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Insufficient arguments")
		return
	}
	address := os.Args[1]
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		fmt.Println(err)
	}
	key := "8"
	var value string

	client.Call("RPCNode.Retrieve", &key, &value)
	client.Close()
	fmt.Printf("%q %q\n", key, value)
}
