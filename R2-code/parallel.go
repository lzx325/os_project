package main

import (
	"fmt"
	"sync"
)

var Workload int = 500000000
var NumWorkloads int = 4

func main() {
	sequentialWork(Workload, NumWorkloads)
	// prallelWork(Workload, NumWorkloads)
}

func work(workload int) {
	x := 1
	for i := 1; i < workload; i++ {
		x = x * i
		x = x / i
	}
	fmt.Println("x = ", x)
}

func sequentialWork(workload, numWorkloads int) {

	fmt.Println("----- Sequential Work")

	for i := 0; i < numWorkloads; i++ {
		work(workload)
	}
}

func prallelWork(workload, numWorkloads int) {

	fmt.Println("----- Parallel Work")

	var wg sync.WaitGroup

	workAndWait := func(workload int) {
		work(workload)
		wg.Done()
	}

	for i := 0; i < numWorkloads; i++ {
		wg.Add(1)
		go workAndWait(workload)
	}

	// Single use functions
	// for i := 0; i < NumCores; i++ {
	// 	wg.Add(1)
	// 	go func() {
	// 		work(Workload)
	// 		wg.Done()
	// 	}()
	// }

	wg.Wait()
}
