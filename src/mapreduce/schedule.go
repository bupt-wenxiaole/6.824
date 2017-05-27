package mapreduce

import "fmt"
import "sync"
import "fmt"
import "math"

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
type workerStatus struct {
    nTasks int
    concurrent int
}
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
	//执行了两次schedule()，一次是mapphase,一次是reducephase，意味着有两个scheduler
	//两个scheduler之间的scheduler是顺序执行的，一个scheduler内调度的各个task是并行的，注意这里面的并发关系
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	// shecheduler启动一个go去持续读取registerChan并把这个新的worker添加到map里，并将这个key对应的两个状态参数初值
	// 设置为0
	workerStatus := make(map[string] *workerStatus)  
	var mapMutex = &sync.Mutex{}
	go func() {
		for {
			newWorkerName := <- registerChan
			mapMutex.Lock()
			workerStatus[newWorkerName] = &workerStatus{0, 0}
			mapMutex.Unlock()
		}
	}

	
	switch phase {
	case mapPhase: 
		for i, f := range mapFiles {
			//根据负载均衡算法选择合适的worker并且更新worker的状态
			var workerSelected string
			minTasks := 0x7fffffff
			for wk, wks := range workerStatus {
				if wks.concurrent < 1 && wks.nTasks < minTasks {
					minTasks = wks.nTasks
					workerSelected = wk
				}
			}
			//注意DoTask里面封装的是rpc.call()，call()是同步调用，远端执行结束后才会进行下一步
			//这边scheduler使用goroutine并行去调用domap，scheduler可以使用channel构造wait和waitgroup来等待goroutine的结束
			func (wk *Worker) DoTask(arg *DoTaskArgs, _ *struct{}) error
			go DoTask()
	}
			doMap(jobName, i, f, mr.nReduce, mapF)
		}
	case reducePhase:
		for i := 0; i < mr.nReduce; i++ {
			doReduce(mr.jobName, i, mergeName(mr.jobName, i), len(mr.files), reduceF)
		}
	}
	
	fmt.Printf("Schedule: %v phase done\n", phase)

}
