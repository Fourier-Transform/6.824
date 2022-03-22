package mr

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var NumOfMap int
var NumOfReduce int
var TimeoutUpperLimit time.Duration

type Master struct {
	// Your definitions here.
	Lock        sync.Mutex
	MasterState int

	MapTaskQueue              *list.List
	MapTaskBuffer             []MapTask
	SessionIdOfMapTask        []int
	CurrentSessionIdOfMapTask int
	FinishedMapTaskCount      int
	TimerChannels             []chan int

	ReduceTaskQueue              *list.List
	ReduceTaskBuffer             []ReduceTask
	SessionIdOfReduceTask        []int
	CurrentSessionIdofReduceTask int
	FinishedReduceTaskCount      int
	ReduceTimerChannels          []chan int
}

type MapTask struct {
	MapTaskId  int
	TargetPath string
	ResultPath string
	//0:master->worker, 1:worker->master
	TaskState int
}

type ReduceTask struct {
	ReduceTaskId int
	TaskState    int
}

type MyError struct {
	State bool
}

func (e MyError) Error() string {
	return "\nthis is my error\n"
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AssignTask(args *WorkerMessage, reply *MasterMessage) error {
	m.Lock.Lock()
	defer m.Lock.Unlock()
	if m.MasterState == 1 {
		if m.MapTaskQueue.Len() == 0 {
			fmt.Println("MapTaskQueue is empty!")
			reply.MasterMessageType = 2
			return nil
		}
		mapTask := m.MapTaskQueue.Front().Value.(MapTask)
		m.MapTaskQueue.Remove(m.MapTaskQueue.Front())
		reply.MasterMessageType = 1
		reply.TheMapTask = mapTask
		reply.TaskType = 1
		reply.SessionId = m.CurrentSessionIdOfMapTask
		m.SessionIdOfMapTask[mapTask.MapTaskId] = m.CurrentSessionIdOfMapTask
		m.CurrentSessionIdOfMapTask++
		fmt.Printf("the map task %d is called\n", mapTask.MapTaskId)
		go m.StartTiming(mapTask.MapTaskId)
	} else if m.MasterState == 2 {
		//通知worker关闭 1->正常回复 2->休眠 3->关闭
		// 此处State为暂时值
		if m.ReduceTaskQueue.Len() == 0 {
			fmt.Println("ReduceTaskQueue is empty!")
			reply.MasterMessageType = 2
			return nil
		}
		reduceTask := m.ReduceTaskQueue.Front().Value.(ReduceTask)
		m.ReduceTaskQueue.Remove(m.ReduceTaskQueue.Front())
		reply.MasterMessageType = 1
		reply.TheReduceTask = reduceTask
		reply.TaskType = 2
		reply.SessionId = m.CurrentSessionIdofReduceTask
		m.SessionIdOfReduceTask[reduceTask.ReduceTaskId] = m.CurrentSessionIdofReduceTask
		m.CurrentSessionIdofReduceTask++
		fmt.Printf("the reduce task %d is called\n", reduceTask.ReduceTaskId)
		go m.StartReduceTiming(reduceTask.ReduceTaskId)
	} else if m.MasterState == 3 {
		reply.MasterMessageType = 3
	}
	return nil
}

func (m *Master) ConfirmTask(args *WorkerMessage, reply *MasterMessage) error {
	m.Lock.Lock()
	defer m.Lock.Unlock()

	if args.TaskType != m.MasterState {
		fmt.Printf("args.TaskType != m.MasterState\n")
		reply.MasterMessageType = 4
		return nil
	}

	if args.TaskType == 1 {
		if args.SessionId != m.SessionIdOfMapTask[args.TheMapTask.MapTaskId] {
			fmt.Printf("args.SessionId != m.SessionIdOfMapTask[args.TheMapTask.MapTaskId]\n")
			reply.MasterMessageType = 4
			return nil
		}
		mapTaskId := args.TheMapTask.MapTaskId
		m.TimerChannels[mapTaskId] <- 1
		m.FinishedMapTaskCount++
		fmt.Printf("%v is finished, FinishedMapTaskCount is %v\n", mapTaskId, m.FinishedMapTaskCount)
		if m.FinishedMapTaskCount == NumOfMap {
			m.MasterState++
		}
	} else if args.TaskType == 2 {
		if args.SessionId != m.SessionIdOfReduceTask[args.TheReduceTask.ReduceTaskId] {
			fmt.Printf("args.SessionId != m.SessionIdOfReduceTask[args.TheReduceTask.ReduceTaskId]\n")
			reply.MasterMessageType = 4
			return nil
		}
		reduceTaskId := args.TheReduceTask.ReduceTaskId
		m.ReduceTimerChannels[reduceTaskId] <- 1
		m.FinishedReduceTaskCount++
		fmt.Printf("%v is finished, FinishedReduceTaskCount is %v\n", reduceTaskId, m.FinishedReduceTaskCount)
		if m.FinishedReduceTaskCount == NumOfReduce {
			m.MasterState++
		}
	}
	return nil
}

func (m *Master) StartTiming(MapTaskId int) {
	select {
	case <-m.TimerChannels[MapTaskId]:

		// m.Lock.Lock()
		// defer m.Lock.Unlock()
		// m.FinishedMapTaskCount++
		// if m.FinishedMapTaskCount == NumOfMap {
		// 	m.MasterState++
		// }
	case <-time.After(TimeoutUpperLimit):
		m.Lock.Lock()
		defer m.Lock.Unlock()
		fmt.Printf("\n%v timeout\n", MapTaskId)
		mapTask := m.MapTaskBuffer[MapTaskId]
		fmt.Printf("len of MapTaskQueue before%v\n", m.MapTaskQueue.Len())
		m.MapTaskQueue.PushBack(mapTask)
		fmt.Printf("len of MapTaskQueue after%v\n", m.MapTaskQueue.Len())
		fmt.Printf("ths mapTaskId is %v\n", MapTaskId)
		fmt.Printf("The queue is: ")
		for e := m.MapTaskQueue.Front(); e != nil; e = e.Next() {
			fmt.Printf("%v ", e.Value)
		}
		fmt.Printf("\n\n")
	}
}

func (m *Master) StartReduceTiming(ReduceTaskId int) {
	select {
	case <-m.ReduceTimerChannels[ReduceTaskId]:
	case <-time.After(TimeoutUpperLimit):
		m.Lock.Lock()
		defer m.Lock.Unlock()
		reduceTask := m.ReduceTaskBuffer[ReduceTaskId]
		m.ReduceTaskQueue.PushBack(reduceTask)
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.MasterState == 3 {
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	//initialize
	for _, v := range files {
		fmt.Println(v)
	}
	NumOfMap = len(files)
	NumOfReduce = nReduce
	TimeoutUpperLimit = 10 * time.Second
	m.MasterState = 0
	m.MapTaskQueue = list.New()
	m.MapTaskBuffer = make([]MapTask, NumOfMap+5)
	for i, v := range files {
		tmp := MapTask{i + 1, v, "", 0}
		m.MapTaskQueue.PushBack(tmp)
		m.MapTaskBuffer[i+1] = tmp
	}
	m.SessionIdOfMapTask = make([]int, 5+NumOfMap)
	m.CurrentSessionIdOfMapTask = 0
	m.FinishedMapTaskCount = 0
	m.TimerChannels = make([]chan int, NumOfMap+5)
	for i := range m.TimerChannels {
		m.TimerChannels[i] = make(chan int)
	}

	m.ReduceTaskQueue = list.New()
	m.ReduceTaskBuffer = make([]ReduceTask, NumOfReduce+5)
	for i := 0; i < NumOfReduce; i++ {
		m.ReduceTaskQueue.PushBack(ReduceTask{i, 0})
		m.ReduceTaskBuffer[i] = ReduceTask{i, 0}
	}
	m.SessionIdOfReduceTask = make([]int, NumOfReduce+5)
	m.CurrentSessionIdofReduceTask = 0
	m.FinishedReduceTaskCount = 0
	m.ReduceTimerChannels = make([]chan int, NumOfReduce+5)
	for i := range m.ReduceTimerChannels {
		m.ReduceTimerChannels[i] = make(chan int)
	}
	m.MasterState = 1
	m.server()
	return &m
}
