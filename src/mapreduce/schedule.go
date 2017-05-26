package mapreduce

import "fmt"
import "sync"

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {

	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)


	//master维护的是worker的各种信息，scheduler负责从worker列表中根据算法来挑选worker执行task
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	// shecheduler启动一个go去持续读取registerChan并维护一个当前最近的worker列表
	workerList []string 
	var listMutex = &sync.Mutex{}
	go func() {
		for {
			newWorkerName := <- registerChan
			listMutex.Lock()
			workerList := append(workerList, newWorkerName)
			listMutex.Unlock()
		}
	//接下来shedule根据自己的phase来将这些任务进行调度，对每个map任务，读取一遍workerlist从上面RPC获取参数，根据参数做负载均衡执选出
	//对应的worker调用dotask

	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
