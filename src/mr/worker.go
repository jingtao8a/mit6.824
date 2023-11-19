package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "github.com/google/uuid"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KeyValueArray []KeyValue

func (a KeyValueArray) Len() int           { return len(a) }
func (a KeyValueArray) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (a KeyValueArray) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func saveKV(kvs []KeyValue, file *os.File) error {
	enc := json.NewEncoder(file)
	for _, kv := range kvs {
		err := enc.Encode(&kv)
		if err != nil {
			return err
		}
	}
	return nil
}

func loadKV(kvs *[]KeyValue, file *os.File) error {
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		*kvs = append(*kvs, kv)
	}
	return nil
}

func resolveMapTask(mapf func(string, string) []KeyValue, getTaskRes GetTaskResponse) ([]string, error) {
	fileName := getTaskRes.Task.InputFileNames[0]
	f, err := os.Open(fileName)
	if err != nil {
		log.Printf("can't open file %s", fileName)
		return nil, err
	}
	contents, err := ioutil.ReadAll(f)
	if err != nil {
		log.Printf("ioutil.ReadAll error %v", err)
		return nil, err
	}
	kvs := mapf(fileName, string(contents))

	intermediateKVs := make([][]KeyValue, getTaskRes.NReduce)

	for _, kv := range kvs {
		reduceID := ihash(kv.Key) % getTaskRes.NReduce
		intermediateKVs[reduceID] = append(intermediateKVs[reduceID], kv)
	}

	outputPaths := []string{}
	uid := uuid.New().String()[:8]
	for reduceID, kvs := range intermediateKVs {
		interFileName := fmt.Sprintf("mr-%v-%v-%v", getTaskRes.Task.TaskID, reduceID, uid)
		f, err := os.Create(interFileName)
		if err != nil {
			log.Printf("create file %s fail", interFileName)
			return nil, err
		}
		err = saveKV(kvs, f)
		if err != nil {
			log.Println("saveKV wrong")
			return nil, err
		}
		outputPaths = append(outputPaths, interFileName)
	}

	return outputPaths, nil
}

func resolveReduceTask(reducef func(string, []string) string, getTaskRes GetTaskResponse) ([]string, error) {
	kvs := []KeyValue{}
	for _, file := range getTaskRes.Task.InputFileNames {
		f, err := os.Open(file)
		if err != nil {
			log.Printf("cannot open file %v, err: %v", f, err)
			return nil, err
		}
		err = loadKV(&kvs, f)
		if err != nil {
			return nil, err
		}
	}

	sort.Sort(KeyValueArray(kvs))

	outputPath := fmt.Sprintf("mr-out-%v", getTaskRes.Task.TaskID)

	outputFile, err := os.Create(outputPath)
	if err != nil {
		return nil, err
	}

	defer outputFile.Close()
	for i := 0; i < len(kvs); {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		fmt.Fprintf(outputFile, "%v %v\n", kvs[i].Key, reducef(kvs[i].Key, values))
		i = j
	}
	return []string{outputPath}, nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	log.SetOutput(ioutil.Discard)
	log.Println("worker start")
	// Your worker implementation here.
	for {
		getTaskReq := GetTaskRequest{}
		getTaskRes := GetTaskResponse{}

		call("Master.GetTask", &getTaskReq, &getTaskRes)
		log.Println("call Master.GetTask")
		log.Println(getTaskRes.Task.TaskID)
		if getTaskRes.ShouldExit {
			//所有task已经完成
			log.Fatal("all task have finished, worker exit")
		}

		if !getTaskRes.Success {
			//没有获得task 等待1000 ms之后继续getTask
			time.Sleep(1000 * time.Millisecond)
			log.Println("worker didn't get task")
			continue
		}

		completeTaskRequest := CompleteTaskRequest{
			Task: getTaskRes.Task,
		}
		completeTaskResponse := CompleteTaskResponse{}
		switch getTaskRes.Task.TaskType {
		case MapTaskType:
			outputPaths, err := resolveMapTask(mapf, getTaskRes)
			if err != nil {
				log.Printf("resolveMapTask fail, error %v", err)
				continue
			}
			completeTaskRequest.Task.OutputFileNames = outputPaths
			break
		case ReduceTaskType:
			outputPaths, err := resolveReduceTask(reducef, getTaskRes)
			if err != nil {
				log.Printf("resolveReduceTask fail, error %v", err)
				continue
			}
			completeTaskRequest.Task.OutputFileNames = outputPaths
			break
		default:
			//assert never reach
			log.Fatal("wrong taskType")
		}

		call("Master.CompleteTaskReq", &completeTaskRequest, &completeTaskResponse)
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
