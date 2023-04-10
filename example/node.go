package main

import (
	"fmt"
	"os"
	"os/signal"

	chord "github.com/kateposp/dht-chord"
)

func main() {
	joinAddress := ""
	address := "127.0.0.1:35383"

	node, err := chord.CreateNewNode(address, joinAddress)
	var a string

	arr := make([]string, 0)
	arr = append(arr, "key")
	arr = append(arr, "hot")
	node.Save(chord.KeyValue{
		Key:   "key",
		Value: []byte("hot"),
	}, &a)

	if err != nil {
		fmt.Println(err)
		return
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	close(c)
	node.Stop()
}
