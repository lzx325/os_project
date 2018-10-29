package main

import (
	"fmt"
	"time"
)

var numHi = 10

func main() {

	fmt.Printf("Saying hi %v times\n", numHi)

	for i := 0; i < numHi; i++ {
		go fmt.Println("Hi!")
	}

	// Once main exits, all go routines are terminated. Need to sleep
	time.Sleep(100 * time.Millisecond)
}
