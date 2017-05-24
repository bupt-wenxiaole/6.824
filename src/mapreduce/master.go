package mapreduce

//
// Please do not modify this file.
//

import (
	"fmt"
	"net"
	"sync"
)

// Master holds all the state that the master needs to keep track of.
type Master struct {
	sync.Mutex

	address     string
	doneChannel chan bool

	// protected by the mutex
	newCond *sync.Cond // signals when Register() adds to workers[]
	workers []string   // each worker's UNIX-domain socket name -- its RPC address

	// Per-task information
	jobName string   // Name of currently executing job
	files   []string // Input files
	nReduce int      // Number of reduce partitions

	shutdown chan struct{}
	l        net.Listener
	stats    []int
}
//下面的这段意思是Register是一种RPC方法，这种RPC方法的server是master(谁注册谁就是这种方法的server)。
//当worker启动就绪可以执行任务的时候worker远程去call这种方法来向master报告其可以接受任务
//同时worker也注册自己要执行的任务，(worker作为RPC server)master通过schedule来call worker执行任务，具体在worker.go中
//TODO:read golang rpc documents and cocurrency documents

// Register is an RPC method that is called by workers after they have started
// up to report that they are ready to receive tasks.
func (mr *Master) Register(args *RegisterArgs, _ *struct{}) error {
	mr.Lock()
	defer mr.Unlock()
	debug("Register: worker %s\n", args.Worker)
	mr.workers = append(mr.workers, args.Worker)

	// tell forwardRegistrations() that there's a new workers[] entry.
	mr.newCond.Broadcast()

	return nil
}

// newMaster initializes a new Map/Reduce Master
func newMaster(master string) (mr *Master) {
	mr = new(Master)
	mr.address = master
	mr.shutdown = make(chan struct{})
	mr.newCond = sync.NewCond(mr)
	mr.doneChannel = make(chan bool)


	return
}

// Sequential runs map and reduce tasks sequentially, waiting for each task to
// complete before running the next.
func Sequential(jobName string, files []string, nreduce int,
	mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string,
) (mr *Master) {
	mr = newMaster("master")
	go mr.run(jobName, files, nreduce, func(phase jobPhase) {  //注意这种写法，传入函数指针的时候同时传入实现
		switch phase {
		case mapPhase:
			for i, f := range mr.files {
				doMap(mr.jobName, i, f, mr.nReduce, mapF)
			}
		case reducePhase:
			for i := 0; i < mr.nReduce; i++ {
				doReduce(mr.jobName, i, mergeName(mr.jobName, i), len(mr.files), reduceF)
			}
		}
	}, func() { //finish函数
		mr.stats = []int{len(files) + nreduce}
	})
	return
}

// helper function that sends information about all existing
// and newly registered workers to channel ch. schedule()
// reads ch to learn about workers.
func (mr *Master) forwardRegistrations(ch chan string) {
	i := 0
	for {
		mr.Lock()
		if len(mr.workers) > i {
			// there's a worker that we haven't told schedule() about.
			w := mr.workers[i]
			go func() { ch <- w }() // send without holding the lock.
			i = i + 1
		} else {
			// wait for Register() to add an entry to workers[]
			// in response to an RPC from a new worker.
			mr.newCond.Wait()
		}
		mr.Unlock()
	}
}

// Distributed schedules map and reduce tasks on workers that register with the
// master over RPC.
func Distributed(jobName string, files []string, nreduce int, master string) (mr *Master) {
	mr = newMaster(master)
	mr.startRPCServer()
	go mr.run(jobName, files, nreduce,
		func(phase jobPhase) {
			ch := make(chan string)
			go mr.forwardRegistrations(ch)
			schedule(mr.jobName, mr.files, mr.nReduce, phase, ch)
		},
		func() {
			mr.stats = mr.killWorkers()
			mr.stopRPCServer()
		})
	return
}

// run executes a mapreduce job on the given number of mappers and reducers.
//
// First, it divides up the input file among the given number of mappers, and
// schedules each task on workers as they become available. Each map task bins
// its output in a number of bins equal to the given number of reduce tasks.
// Once all the mappers have finished, workers are assigned reduce tasks.
//
// When all tasks have been completed, the reducer outputs are merged,
// statistics are collected, and the master is shut down.
//
// Note that this implementation assumes a shared file system.
func (mr *Master) run(jobName string, files []string, nreduce int,
	schedule func(phase jobPhase),
	finish func(),
) {
	mr.jobName = jobName
	mr.files = files
	mr.nReduce = nreduce

	fmt.Printf("%s: Starting Map/Reduce task %s\n", mr.address, mr.jobName)

	schedule(mapPhase)  //对于sequential的情况下，传入两个常量调用两次，相当于顺序执行map任务和reduce任务，sequential情况下传入的是在
	//传入时实现的函数
	schedule(reducePhase)
	finish()
	mr.merge()

	fmt.Printf("%s: Map/Reduce task completed\n", mr.address)

	mr.doneChannel <- true
}

// Wait blocks until the currently scheduled work has completed.
// This happens when all tasks have scheduled and completed, the final output
// have been computed, and all workers have been shut down.
func (mr *Master) Wait() {
	<-mr.doneChannel
}

// killWorkers cleans up all workers by sending each one a Shutdown RPC.
// It also collects and returns the number of tasks each worker has performed.
func (mr *Master) killWorkers() []int {
	mr.Lock()
	defer mr.Unlock()
	ntasks := make([]int, 0, len(mr.workers))
	for _, w := range mr.workers {
		debug("Master: shutdown worker %s\n", w)
		var reply ShutdownReply
		ok := call(w, "Worker.Shutdown", new(struct{}), &reply)
		if ok == false {
			fmt.Printf("Master: RPC %s shutdown error\n", w)
		} else {
			ntasks = append(ntasks, reply.Ntasks)
		}
	}
	return ntasks
}
