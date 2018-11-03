package main

import "fmt"
import "time"

import "sync"

func main() {
	timer := time.NewTimer(500 * time.Millisecond)
	mu := sync.Mutex{}
	go func() {
		fmt.Println("aaa")
		time.Sleep(3000 * time.Millisecond)
		mu.Lock()
		if !timer.Stop() {
			<-timer.C
		}
		timer.Reset(1000 * time.Millisecond)
		mu.Unlock()
	}()
	<-timer.C
	fmt.Println("bbb")
	<-timer.C
	fmt.Println("ccc")
}
