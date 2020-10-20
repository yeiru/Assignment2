package mapreduce

import (
	"fmt"
	"sync"
	"strconv"
)
var wg sync.WaitGroup

func processWork(mr *Master, worker string, args DoTaskArgs, availableWork chan DoTaskArgs) {
	ok := call(worker, "Worker.DoTask", args, new(struct{}))
	if ok {
		wg.Done()	
		mr.registerChannel <- worker
	} else {
		fmt.Println("No response from server, executing task again " + worker + " TASK: " + strconv.Itoa(args.TaskNumber))
		availableWork <- args
	}
}

func waitForWorker(mr *Master, availableWork chan DoTaskArgs) {
	for {
		worker := <- mr.registerChannel
		nextTask := <- availableWork		
		go processWork(mr, worker, nextTask, availableWork) 
	}
}

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
	// fmt.Println(mr.files)
	
	
	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	availableWork := make(chan DoTaskArgs, ntasks)
	for task := 0; task < ntasks; task++ {
		args := DoTaskArgs{
			JobName: mr.jobName,
			File: mr.files[task],
			Phase: phase,
			TaskNumber: task,
			NumOtherPhase: nios,
		}
		availableWork <- args
		wg.Add(1)
	}

	go waitForWorker(mr, availableWork) 	

	wg.Wait()
	debug("Schedule: %v phase done\n", phase)
}
