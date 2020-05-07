package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		taskInfo := CallGetTask()
		switch taskInfo.TaskType {
		case TaskMap:
			ok, files := runMapTask(taskInfo.Param, taskInfo.TaskId, taskInfo.ReduceN, mapf)
			CallMapResult(ok, taskInfo.TaskId, files)
		case TaskReduce:
			ok := runReduceTask(taskInfo.TaskId, taskInfo.Param, reducef)
			CallReduceResult(ok, taskInfo.TaskId)
		case TaskExit:
			time.Sleep(time.Second * 5)
			os.Exit(0)
		default:
			// fmt.Println("sleep")
			time.Sleep(time.Millisecond * 50)
		}
	}
}

// CallGetTask 从 master 获取任务
func CallGetTask() *GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Master.GetTask", &args, &reply)
	// fmt.Printf("CallGetTask reply: %+v\n", reply)
	return &reply
}

func CallMapResult(ok bool, id int, files []string) {
	args := MapResultArgs{
		Ok:          ok,
		Id:          id,
		ReduceFiles: files,
	}
	reply := ResultReply{}
	call("Master.MapResult", &args, &reply)
}

func CallReduceResult(ok bool, id int) {
	args := ReduceResultArgs{
		Ok: ok,
		Id: id,
	}
	reply := ResultReply{}
	call("Master.ReduceResult", &args, &reply)
}

// runMapTask 执行map任务
func runMapTask(filename string, id int, r int, mapf func(string, string) []KeyValue) (bool, []string) {
	// 读取 map 传入文件
	data, err := ioutil.ReadFile(filename)
	// fmt.Println(string(data))
	if err != nil {
		return false, nil
	}
	kvs := mapf(filename, string(data))

	// 创建 map 任务中间文件（mr-{mapId}-{reduceN}.txt）
	files := make([]string, r)
	for i := range files {
		files[i] = fmt.Sprintf("mr-%d-%d.txt", id, i)
	}
	// fmt.Printf("files: %+v \n ", files)

	// 创建 map 任务写入 json encoder 数组
	encodes := make([]*json.Encoder, r)
	fileWriters := make([]*os.File, r)
	for i := range encodes {
		// os.OpenFile(files[i], os.O_CREATE|os.O_WRONLY, os.ModePerm)
		fileWriters[i], err = ioutil.TempFile("", files[i])
		if err != nil {
			fmt.Println(err)
			return false, nil
		}
		encodes[i] = json.NewEncoder(fileWriters[i])
	}
	defer func() {
		for _, f := range fileWriters {
			_ = f.Close()
		}
	}()
	// fmt.Printf("fileWriters: %+v \n ", fileWriters)

	// 根据 ihash 对 key 的处理结果与 reduce 数量取模写入特定文件
	for _, kv := range kvs {
		k := ihash(kv.Key) % r
		err = encodes[k].Encode(&kv)
		if err != nil {
			fmt.Println(err)
			return false, nil
		}
	}
	for i, f := range fileWriters {
		if err := f.Close(); err != nil {
			fmt.Println(err)
		}
		if err := os.Rename(f.Name(), files[i]); err != nil {
			fmt.Println(err)
		}
	}
	return true, files
}

func readFile(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	defer func() {
		err := file.Close()
		if err != nil {
			fmt.Println(err)
		}
	}()
	if err != nil {
		return nil, err
	}
	var kva []KeyValue
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva, nil
}

func runReduceTask(id int, filenames string, reducef func(string, []string) string) bool {
	files := strings.Split(filenames, ",")
	kva := make([]KeyValue, 1000)
	for _, f := range files {
		kvs, err := readFile(f)
		if err != nil {
			return false
		}
		kva = append(kva, kvs...)
	}
	sort.Sort(ByKey(kva))
	outFile := fmt.Sprintf("mr-out-%d", id)
	// onfile, err := os.Create(outFile)
	onfile, err := ioutil.TempFile("", outFile)
	defer func() {
		_ = onfile.Close()
	}()
	if err != nil {
		fmt.Println(err)
		return false
	}
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		if kva[i].Key != "" {
			_, err = fmt.Fprintf(onfile, "%v %v\n", kva[i].Key, output)
			if err != nil {
				fmt.Println(err)
				return false
			}
		}
		i = j
	}
	if err := onfile.Close(); err != nil {
		fmt.Println(err)
	}
	if err := os.Rename(onfile.Name(), outFile); err != nil {
		fmt.Println(err)
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
