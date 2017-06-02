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
type workerStatus struct {
    nTasks int
    concurrent int
}

func doTaskWrapper(arg *DoTaskArgs, workerName string, m *sync.Mutex, mapWS map[string] *workerStatus, wg *sync.WaitGroup) {
	//Maps are reference types, so they are always passed by reference. You don't need a pointer.
	//call is synchronous
	if wg == nil{
		fmt.Println("fuck it")
	}
	defer wg.Done()
	m.Lock()
	mapWS[workerName].concurrent++  //这句访问空指针，不是因为mapWS引用没传进来，也不是因为mapWS没有初始化。具体可见下面这个例子
	//https://play.golang.org/p/reEpyyfxn8
	//golang map 有个特性，如果直接访问不存在的key对应的value，仍然会有返回值，不过是一个默认的未初始化值，例如int是0。指针为空
	//正规的写法在检测map的key之前检查key是否存在于map当中
	//还有一个要fix的问题就是wrong number ins : 1的问题，现在想的就是通过把public的方法包装成私有的方法，let's try it
	m.Unlock()
	var empty struct{}
	ok := call(workerName, "Worker.DoTask", arg, &empty)
	if ok == false {
  		fmt.Printf("doTaskWrapper: RPC %s shutdown error\n", workerName)
  		return 
  		//TODO: if the RPC fails, do something
  	}
  	m.Lock()
  	mapWS[workerName].concurrent--
  	mapWS[workerName].nTasks++
  	m.Unlock()

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
	mapWorkerStatus := make(map[string] *workerStatus)  
	var mapMutex = &sync.Mutex{}
	go func() {
		for {
			newWorkerName := <- registerChan
			//TODO: THIS MAY CAUSE DEADLOCK
			mapMutex.Lock()
			mapWorkerStatus[newWorkerName] = &workerStatus{0, 0}
			mapMutex.Unlock()
		}
	}()

	switch phase {
	case mapPhase:
		var wg sync.WaitGroup 
		for i, f := range mapFiles {
			//根据负载均衡算法选择合适的worker并且更新worker的状态
			var workerSelected string
			minTasks := 0x7fffffff
			mapMutex.Lock()
			for wk, wks := range mapWorkerStatus {
				if wks.concurrent < 1 && wks.nTasks < minTasks {
					minTasks = wks.nTasks
					workerSelected = wk
				}
			}
			mapMutex.Unlock()
			//注意DoTask里面封装的是rpc.call()，call()是同步调用，远端执行结束后才会进行下一步
			//这边scheduler使用goroutine并行去调用domap，scheduler可以使用channel构造wait和waitgroup来等待goroutine的结束
			//设置一个goroutine专门去等待各个woker执行结束后对两个参数进行修改，防止在主线程中等待，用一个函数把
			//dotask和这个发射信号的函数包裹起来统一用一个goroutine去执行
			//注意现在这个版本默认远程RPC是可以成功，不存在失败的情况
			//关于golang等待goroutine 退出
			//https://stackoverflow.com/questions/18207772/how-to-wait-for-all-goroutines-to-finish-without-using-time-sleep

			var taskArgs *DoTaskArgs = &DoTaskArgs{jobName, f, phase, i, nReduce}
			wg.Add(1)
			go doTaskWrapper(taskArgs, workerSelected, mapMutex, mapWorkerStatus, &wg)

		}
		wg.Wait()
	case reducePhase:
		var wg sync.WaitGroup 
		for i := 0; i < nReduce; i++ {
			var workerSelected string
			minTasks := 0x7fffffff
			mapMutex.Lock()
			for wk, wks := range mapWorkerStatus {
				if wks.concurrent < 1 && wks.nTasks < minTasks {
					minTasks = wks.nTasks
					workerSelected = wk
				}
			}
			mapMutex.Unlock()
			var taskArgs *DoTaskArgs = &DoTaskArgs{jobName, "", phase, i, n_other}
			wg.Add(1)
			go doTaskWrapper(taskArgs, workerSelected, mapMutex, mapWorkerStatus, &wg)
		}
		wg.Wait()
	}
	fmt.Printf("Schedule: %v phase done\n", phase)

}
