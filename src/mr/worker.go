package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// uncomment to send the Example RPC to the master.
	// CallExample()
	taskInfo := CallGetTask()
	taskResult := false
	switch taskInfo.TaskType {
	case "Map":
		taskResult = runMapTask(taskInfo.Key, taskInfo.MapId, taskInfo.ReduceN, mapf)
	case "Reduce":
	default:
	}
	fmt.Println(taskResult)

}

// CallGetTask 从 master 获取任务
func CallGetTask() *GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Master.GetTask", &args, &reply)
	fmt.Printf("CallGetTask reply: %+v\n", reply)
	return &reply
}

// runMapTask 执行map任务
func runMapTask(filename string, id int, r int, mapf func(string, string) []KeyValue) bool {
	// 读取 map 传入文件
	data, err := ioutil.ReadFile(filename)
	// fmt.Println(string(data))
	if err != nil {
		return false
	}
	kvs := mapf(filename, string(data))

	// 创建 map 任务中间文件（mr-tmp/mr-{mapId}-{reduceN}.txt）
	files := make([]string, r)
	for i := range files {
		files[i] = fmt.Sprintf("/tmp/mr-%d-%d.txt", id, i)
	}
	// fmt.Printf("files: %+v \n ", files)

	// 创建 map 任务写入 json encoder 数组
	encodes := make([]*json.Encoder, r)
	fileWriters := make([]*os.File, r)
	defer func() {
		for _, f := range fileWriters {
			_ = f.Close()
		}
	}()
	for i := range encodes {
		fileWriters[i], err = os.OpenFile(files[i], os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			fmt.Println(err)
			return false
		}
		encodes[i] = json.NewEncoder(fileWriters[i])
	}
	// fmt.Printf("fileWriters: %+v \n ", fileWriters)

	// 根据 ihash 对 key 的处理结果与 reduce 数量取模写入特定文件
	for _, kv := range kvs {
		k := ihash(kv.Key) % r
		err = encodes[k].Encode(&kv)
		if err != nil {
			fmt.Println(err)
			return false
		}
	}
	return true
}

// CallExample example function to show how to make an RPC call to the master. the RPC argument and reply types are defined in rpc.go.
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
	fmt.Printf("reply.Y %+v\n", reply.Y)
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
	defer func() {
		_ = c.Close()
	}()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
