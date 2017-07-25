package mapreduce

//
// Please do not modify this file.
//

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// Worker holds the state for a server waiting for DoTask or Shutdown RPCs
type Worker struct {
	mutex *sync.Mutex //注意这种写法，使用了Golang的匿名字段和内嵌结构体
	//Go中的继承是通过内嵌或组合来实现的

	name   string
	Map    func(string, string) []KeyValue
	Reduce func(string, []string) string
	nRPC   int // quit after this many RPCs; protected by mutex  在nRPC次RPC调用后worker退出，
	//如果初值传入-1的话则意味着worker可以无限次执行RPC调用，注意这种写法
	nTasks     int // total tasks executed; protected by mutex
	concurrent int // number of parallel DoTasks in this worker; mutex
	l          net.Listener
	doneChan   chan bool
	listenChan chan net.Conn
}

func (wk *Worker) lock() {
	wk.mutex.Lock()
}

func (wk *Worker) unlock() {
	wk.mutex.Unlock()
}

// DoTask is called by the master when a new task is being scheduled on this
// worker.
func (wk *Worker) DoTask(arg *DoTaskArgs, _ *struct{}) error {
	// fmt.Printf("%s: given %v task #%d on file %s (nios: %d)\n",
	// wk.name, arg.Phase, arg.TaskNumber, arg.File, arg.NumOtherPhase)

	wk.lock()
	wk.nTasks += 1
	wk.concurrent += 1
	nc := wk.concurrent
	wk.unlock()

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

	wk.lock()
	wk.concurrent -= 1
	wk.unlock()

	fmt.Printf("%s: %v task #%d done\n", wk.name, arg.Phase, arg.TaskNumber)
	return nil
}

// Shutdown is called by the master when all work has been completed.
// We should respond with the number of tasks we have processed.
func (wk *Worker) Shutdown(_ *struct{}, res *ShutdownReply) error {
	debug("Shutdown %s\n", wk.name)
	wk.lock()
	defer wk.unlock()
	res.Ntasks = wk.nTasks
	wk.nRPC = 1 //在master端调用shutdown之后work还能接受一次远端的调用
	wk.doneChan <- true
	return nil
}

// Tell the master we exist and ready to work
func (wk *Worker) register(master string) {
	fmt.Println(wk.name, ": Try to call Master.Register: ")
	for i := 0; i < 30; i++ {
		args := new(RegisterArgs)
		args.Worker = wk.name
		ok := call(master, "Master.Register", args, new(struct{}))
		if ok == false {
			time.Sleep(1000 * time.Millisecond)
		} else {
			fmt.Println(wk.name, ": Call Master.Register done.")
			return
		}
	}
	log.Fatal("%s: Register: RPC %s register error\n", wk.name, master)
}

// RunWorker sets up a connection with the master, registers its address, and
// waits for tasks to be scheduled.
// the first para should be named as address because the usage
func RunWorker(Address string, me string,
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
	wk.doneChan = make(chan bool)
	wk.listenChan = make(chan net.Conn)
	wk.mutex = new(sync.Mutex)

	rpcs := rpc.NewServer() //worker启动自己的RPC服务
	rpcs.Register(wk)
	l, e := net.Listen("tcp", ":7778")
	if e != nil {
		log.Fatal("RunWorker: worker ", me, " error: ", e)
	}
	wk.l = l
	wk.register(Address)

	// DON'T MODIFY CODE BELOW
loop:
	for {
		wk.lock()
		if wk.nRPC == 0 {
			wk.unlock()
			break loop
		}
		wk.unlock()
		go wk.listenAndChan(rpcs)
		select {
		case conn := <-wk.listenChan:
			wk.lock()
			wk.nRPC--
			wk.unlock()
			go rpcs.ServeConn(conn)
		case <-wk.doneChan:
			break loop
		}
	}

	wk.l.Close()
	debug("RunWorker %s exit\n", me)
}

func (wk *Worker) listenAndChan(rpcs *rpc.Server) {
	conn, err := wk.l.Accept()
	if err == nil {
		wk.listenChan <- conn
	} else {
		wk.doneChan <- true
	}
}