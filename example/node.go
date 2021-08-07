package main

import (
	"fmt"
	"os"
	"os/signal"

	chord "github.com/sethiojas/dht-chord"
)

func main() {
	node, err := chord.CreateNewNode("localhost:12334", "")
	if err != nil {
		fmt.Println(err)
		return
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	node.Stop()
}
