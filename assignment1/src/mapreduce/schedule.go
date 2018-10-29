package mapreduce

import (
	"fmt"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	mr.Lock()
	if len(mr.workers) <= 0 {
		mr.Unlock()
		<-mr.registerChannel
	} else {
		mr.Unlock()
	}

	switch phase {
	case mapPhase:
		for i, f := range mr.files {

			args := &DoTaskArgs{mr.jobName, f, phase, i, nios}
			j := 0
			for j = 0; j < len(mr.workers); j++ {
				// mr.Lock()
				worker := mr.workers[(i+j)%len(mr.workers)]
				// fmt.Println(len(mr.workers))
				// mr.Unlock()
				if call(worker, "Worker.DoTask", args, new(struct{})) {
					fmt.Println(i, f, len(mr.workers), worker)
					break
				}
			}
			if j == len(mr.workers) {
				fmt.Println("All workers have failed on this task,waiting for new workers to come...")
				worker := <-mr.registerChannel
				for call(worker, "Worker.DoTask", args, new(struct{})) != true {
					worker = <-mr.registerChannel
				}
				fmt.Println(i, f, len(mr.workers), worker)
			}
		}
	case reducePhase:
		for i := 0; i < ntasks; i++ {
			args := &DoTaskArgs{mr.jobName, "", phase, i, nios}
			j := 0
			for j = 0; j < len(mr.workers); j++ {
				worker := mr.workers[(i+j)%len(mr.workers)]
				if call(worker, "Worker.DoTask", args, new(struct{})) {
					break
				}
			}
			if j == len(mr.workers) {
				fmt.Println("All workers have failed on this task,waiting for new workers to come...")
				worker := <-mr.registerChannel
				for call(worker, "Worker.DoTask", args, new(struct{})) != true {
					worker = <-mr.registerChannel
				}
			}

		}

	}
	fmt.Printf("Schedule: %v phase done\n", phase)
	debug("Schedule: %v phase done\n", phase)
}
