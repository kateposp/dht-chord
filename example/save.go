package main

import (
	"fmt"
	"net/rpc"
)

func main() {

	address := "127.0.0.1:35383"
	arr := make([]string, 0)
	arr = append(arr, "key")
	arr = append(arr, "hot")
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		fmt.Println(err)
	}
	var storeNode string
	client.Call("RPCNode.Save", arr, &storeNode)
	client.Close()
	fmt.Println(storeNode)
}
