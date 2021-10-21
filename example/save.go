package main

import (
	"fmt"
	"net/rpc"
	"os"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Insufficient arguments")
		return
	}
	address := os.Args[1]
	arr := make([]string, 2)
	arr = append(arr, os.Args[2])
	arr = append(arr, os.Args[3])
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		fmt.Println(err)
	}
	var storeNode string
	client.Call("RPCNode.Save", arr, &storeNode)
	client.Close()
	fmt.Println(storeNode)
}
