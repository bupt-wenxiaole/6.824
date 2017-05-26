package mapreduce

//
// Please do not modify this file.
//

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

// Worker holds the state for a server waiting for DoTask or Shutdown RPCs
type Worker struct {
	sync.Mutex  //注意这种写法，使用了Golang的匿名字段和内嵌结构体
	//Go中的继承是通过内嵌或组合来实现的

	name       string
	Map        func(string, string) []KeyValue
	Reduce     func(string, []string) string
	nRPC       int // quit after this many RPCs; protected by mutex  在nRPC次RPC调用后worker退出，
	//如果初值传入-1的话则意味着worker可以无限次执行RPC调用，注意这种写法
	nTasks     int // total tasks executed; protected by mutex
	concurrent int // number of parallel DoTasks in this worker; mutex
	l          net.Listener
}

type StatusReply struct {
	nTasks int
	concurrent int
}

// DoTask is called by the master when a new task is being scheduled on this
// worker.
func (wk *Worker) DoTask(arg *DoTaskArgs, _ *struct{}) error {
	fmt.Printf("%s: given %v task #%d on file %s (nios: %d)\n",
		wk.name, arg.Phase, arg.TaskNumber, arg.File, arg.NumOtherPhase)

	wk.Lock()
	wk.nTasks += 1
	wk.concurrent += 1
	nc := wk.concurrent
	wk.Unlock()

	if nc > 1 {
		// schedule() should never issue more than one RPC at a
		// time to a given worker.
		log.Fatal("Worker.DoTask: more than one DoTask sent concurrently to a single worker\n")
	}

	switch arg.Phase {
	case mapPhase:
		doMap(arg.JobName, arg.TaskNumber, arg.File, arg.NumOtherPhase, wk.Map)
	case reducePhase:
		doReduce(arg.JobName, arg.TaskNumber, mergeName(arg.JobName, arg.TaskNumber), arg.NumOtherPhase, wk.Reduce)
	}

	wk.Lock()
	wk.concurrent -= 1
	wk.Unlock()

	fmt.Printf("%s: %v task #%d done\n", wk.name, arg.Phase, arg.TaskNumber)
	return nil
}
//this func is for getting the worker's status parameters
func (wk *Worker) GetStatus(_ *struct{}, res *StatusReply) error{
	fmt.Printf("%s: schedule() ask me the status parameters\n", wk.name)
	wk.Lock()
	defer wk.Unlock()
	res.nTasks = wk.nTasks
	res.concurrent = wk.concurrent
	return nil

}
// Shutdown is called by the master when all work has been completed.
// We should respond with the number of tasks we have processed.
func (wk *Worker) Shutdown(_ *struct{}, res *ShutdownReply) error {
	debug("Shutdown %s\n", wk.name)
	wk.Lock()
	defer wk.Unlock()
	res.Ntasks = wk.nTasks
	wk.nRPC = 1  //在master端调用shutdown之后work还能接受一次远端的调用
	return nil
}

// Tell the master we exist and ready to work
func (wk *Worker) register(master string) {
	args := new(RegisterArgs)
	args.Worker = wk.name
	ok := call(master, "Master.Register", args, new(struct{}))
	if ok == false {
		fmt.Printf("Register: RPC %s register error\n", master)
	}
}

// RunWorker sets up a connection with the master, registers its address, and
// waits for tasks to be scheduled. 
func  RunWorker(MasterAddress string, me string,
	MapFunc func(string, string) []KeyValue,
	ReduceFunc func(string, []string) string,
	nRPC int,
) {
	debug("RunWorker %s\n", me)
	wk := new(Worker)
	wk.name = me
	wk.Map = MapFunc
	wk.Reduce = ReduceFunc
	wk.nRPC = nRPC
	rpcs := rpc.NewServer() //worker启动自己的RPC服务
	rpcs.Register(wk)
	os.Remove(me) // only needed for "unix"
	l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("RunWorker: worker ", me, " error: ", e)
	}
	wk.l = l
	wk.register(MasterAddress)
	//调用master上的rpc服务

	// DON'T MODIFY CODE BELOW
	for {
		wk.Lock()
		if wk.nRPC == 0 {
			wk.Unlock()
			break
		}
		wk.Unlock()
		conn, err := wk.l.Accept()
		if err == nil {
			wk.Lock()
			wk.nRPC--
			wk.Unlock()
			go rpcs.ServeConn(conn)
		} else {
			break
		}
	}
	wk.l.Close()
	debug("RunWorker %s exit\n", me)
}
