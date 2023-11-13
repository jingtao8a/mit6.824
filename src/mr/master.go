package mr

import (
	"log"
	"sync"
)

import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	MapTaskType    int8 = 0
	ReduceTaskType int8 = 1
)

const (
	PendingTaskStatus  int8 = 0
	RunningTaskStatus  int8 = 1
	CompleteTaskStatus int8 = 2
)

type Task struct {
	taskType        int8
	taskStaus       int8
	taskId          int
	inputFileNames  []string
	outputFileNames []string
}

const (
	MapPhase    int8 = 0
	ReducePhase int8 = 1
)

type Master struct {
	// Your definitions here.
	nReduce                   int
	mapTaskList               []Task
	reduceTaskList            []Task
	inCompleteMapTaskCount    int
	inCompleteReduceTaskCount int
	phase                     int8
	mux                       sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

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

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here
	m.nReduce = nReduce

	for i, file := range files {
		m.mapTaskList = append(m.mapTaskList,
			Task{
				taskType:       MapTaskType,
				taskStaus:      PendingTaskStatus,
				taskId:         i,
				inputFileNames: []string{file},
			})
	}

	for i := 0; i < nReduce; i++ {
		m.reduceTaskList = append(m.reduceTaskList,
			Task{
				taskType:  ReduceTaskType,
				taskStaus: ReducePhase,
				taskId:    i,
			})
	}

	m.inCompleteMapTaskCount = len(files)
	m.inCompleteReduceTaskCount = nReduce
	m.phase = MapPhase
	m.server()
	return &m
}
