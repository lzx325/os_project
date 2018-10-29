package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {

	// basicChannel()
	// bufferedChannel()
	// pingPong()
	// closingChannels()
	// multiChannel()

	// TODO: locks before waitgroups
	// waitGroups()

}

func basicChannel() {

	fmt.Println("----- Basic Channels")

	ch := make(chan int)

	go func() {
		ch <- 2
		fmt.Println("Put: ", 2)
		// ch <- 3
		// fmt.Println("Put: ", 3)

	}()

	x := <-ch
	fmt.Println("Read: ", x)

	// Channel reading blocks until something is there to be read
	// y := <-ch
	// fmt.Println("Read: ", y)
}

func bufferedChannel() {

	fmt.Println("----- Buffered Channels")

	ch := make(chan int, 2)

	go func() {
		fmt.Println("Put: ", 2)
		ch <- 2

		// Notice how the order changes when you uncomment the line below
		// time.Sleep(10 * time.Microsecond)

		fmt.Println("Put: ", 3)
		ch <- 3
	}()

	x := <-ch
	fmt.Println("Read: ", x)

	y := <-ch
	fmt.Println("Read: ", y)
}

func pingPong() {

	fmt.Println("----- Ping Pong Game Begins!")

	var ball int
	table := make(chan int)
	go player(table, "Ping")
	go player(table, "Pong")

	table <- ball
	time.Sleep(1 * time.Second)
	<-table
	fmt.Println("Referee stopped game")
}

func player(table chan int, phrase string) {
	for {
		ball := <-table
		ball++
		time.Sleep(100 * time.Millisecond)
		fmt.Println(phrase, " ", ball)
		table <- ball
	}
}

func closingChannels() {

	fmt.Println("----- Closing Channels")

	ch := make(chan int)

	go func() {
		ch <- 2
		fmt.Println("Put: ", 2)
		close(ch)
		fmt.Println("Closed ch")
	}()

	// Basic way to check if a channel has been closed
	for i := 0; i < 5; i++ {
		x, ok := <-ch
		if ok {
			fmt.Println("ch Closed")
			break
		}
		fmt.Println("Got: ", x)
	}

	// More consice way of reading channel only if it's still open
	// for x := range ch {
	// 	fmt.Println("Got: ", x)
	// }
}

func multiChannel() {

	fmt.Println("----- Waiting on Multiple Channels")

	ch1 := make(chan int)
	ch2 := make(chan int)

	go func() {
		time.Sleep(10 * time.Microsecond)
		ch1 <- 1
	}()

	go func() {
		time.Sleep(1 * time.Microsecond)
		ch2 <- 2
	}()

	select {
	case x := <-ch1:
		fmt.Println("Got: ", x)
	case x := <-ch2:
		fmt.Println("Got: ", x)
		// default:
		// 	fmt.Println("Tired of waiting")
	}
}

func waitGroups() {

	fmt.Println("----- Waiting on A Group of Routines")

	var wg sync.WaitGroup

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			// What happens if the wg.Add(1) is in here?
			time.Sleep(100 * time.Millisecond)
			fmt.Println("done")
			wg.Done()
		}()
	}

	wg.Wait()
}
