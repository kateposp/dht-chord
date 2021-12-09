package main

import (
	"fmt"
	"os"
	"os/signal"

	chord "github.com/sethiojas/dht-chord"
)

func main() {
	args := len(os.Args)
	joinAddress := ""
	if args == 3 {
		joinAddress = os.Args[2]
	} else if args > 3 {
		fmt.Println("More than required number of args")
		return
	} else if args < 2 {
		fmt.Println("Less number of args than required")
		return
	}
	address := os.Args[1]

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
