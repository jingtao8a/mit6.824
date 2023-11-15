package mr

import (
	"log"
	"strconv"
	"strings"
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

func (m *Master) GetTask(request *GetTaskRequest, response *GetTaskResponse) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	(*response).NReduce = m.nReduce
	var tasks []Task
	if m.inCompleteMapTaskCount > 0 {
		tasks = m.mapTaskList
		//} else if m.inCompleteReduceTaskCount > 0 {
		//tasks = m.reduceTaskList
	} else {
		//所有任务已经完成了
		(*response).success = false
		(*response).shouldExit = true
		return nil
	}

	for _, task := range tasks {
		if task.taskStaus == PendingTaskStatus {
			(*response).task = task
			(*response).NReduce = m.nReduce
			(*response).success = true
			(*response).shouldExit = false
			//pendingTaskStatus -> RunningTaskStatus
			task.taskStaus = RunningTaskStatus
			log.Println("assign mapTask")
			//需要添加task超时检测
			//to do
			return nil
		}
	}
	//没有空闲的任务
	(*response).success = false
	(*response).shouldExit = false
	return nil
}

func (m *Master) CompleteTaskReq(request *CompleteTaskRequest, response *CompleteTaskResponse) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	switch request.task.taskType {
	case MapTaskType:
		m.mapTaskList[request.task.taskId].taskStaus = CompleteTaskStatus
		m.mapTaskList[request.task.taskId].outputFileNames = request.task.outputFileNames
		m.inCompleteMapTaskCount--
		if m.inCompleteMapTaskCount == 0 {
			m.phase = ReducePhase
			m.initReduceTaskInput()
		}

		break
	case ReduceTaskType:

		break
	}
	return nil
}

func (m *Master) initReduceTaskInput() {
	reduceTasksInput := make([][]string, m.nReduce)
	for _, task := range m.mapTaskList {
		for _, fileName := range task.outputFileNames {
			tmp := strings.Split(fileName, "-")
			reduceId, err := strconv.Atoi(tmp[2])
			if err != nil {
				log.Fatal("str -> int fail")
			}
			reduceTasksInput[reduceId] = append(reduceTasksInput[reduceId], fileName)
		}
	}

	for reduceID, task := range m.reduceTaskList {
		task.inputFileNames = reduceTasksInput[reduceID]
	}
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
				taskStaus: PendingTaskStatus,
				taskId:    i,
			})
	}

	m.inCompleteMapTaskCount = len(files)
	m.inCompleteReduceTaskCount = nReduce
	m.phase = MapPhase
	m.server()
	log.Println("Master m info")
	log.Printf("m.mapTaskList  len:%d", len(m.mapTaskList))
	log.Printf("m.reduceTaskList len:%d", len(m.reduceTaskList))
	return &m
}
