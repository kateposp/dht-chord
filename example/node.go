package main

import (
	"fmt"
	"os"
	"os/signal"

	chord "github.com/sethiojas/dht-chord"
)

func main() {
	address := os.Args[1]
	joinAddress := ""
	if len(os.Args) >= 2 {
		joinAddress = os.Args[2]
	}
	node, err := chord.CreateNewNode(address, joinAddress)

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
