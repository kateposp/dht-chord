package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"time"

	chord "github.com/sethiojas/dht-chord"
)

func main() {
	node, err := chord.CreateNewNode("localhost:12335", "localhost:12334")
	if err != nil {
		fmt.Println(err)
		return
	}

	ch := make(chan struct{})
	go func() {
		count := 500
		for {
			ticker := time.NewTicker(5 * time.Second)
			select {
			case <-ticker.C:
				count++
				key := strconv.Itoa(count)
				value := "hello-" + strconv.Itoa(count)
				node.Save(key, value)
			case <-ch:
				ticker.Stop()
				return
			}
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	close(ch)
	close(c)
	node.Stop()
}
