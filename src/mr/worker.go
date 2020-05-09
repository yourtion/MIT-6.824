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

// ByKey 用于进行 KeyValue 排序
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use iHash(key) % NReduce to choose the reduce task number for each KeyValue emitted by Map.
func iHash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapFun func(string, string) []KeyValue, reduceFun func(string, []string) string) {
	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		taskInfo := CallGetTask()
		// 任务执行完成后等待 1s 再继续请求，否则可能会导致 parallelism test 任务失败（被同一个 worker 做完所有任务）
		switch taskInfo.TaskType {
		case TaskMap:
			// 执行 Map 任务并返回对应的 Reduce 任务文件列表
			ok, files := runMapTask(taskInfo.Param, taskInfo.TaskId, taskInfo.ReduceN, mapFun)
			CallMapResult(ok, taskInfo.TaskId, files)
			time.Sleep(time.Second)
		case TaskReduce:
			// 执行 Reduce 任务并返回结果
			ok := runReduceTask(taskInfo.TaskId, taskInfo.Param, reduceFun)
			CallReduceResult(ok, taskInfo.TaskId)
			time.Sleep(time.Second)
		case TaskExit:
			// 等待 5s 并退出
			time.Sleep(time.Second * 5)
			os.Exit(0)
		default:
			// 没有任务，等待 0.5s 再次请求
			time.Sleep(time.Millisecond * 500)
		}
	}
}

// CallGetTask 从 master 获取任务
func CallGetTask() *GetTaskReply {
	args := GetTaskArgs{WorkerId: os.Getpid()}
	reply := GetTaskReply{}
	call("Master.GetTask", &args, &reply)
	DLog("CallGetTask reply: %+v\n", reply)
	return &reply
}

// CallMapResult 通知 master Map 任务已经完成
func CallMapResult(ok bool, mapId int, files []string) {
	args := MapResultArgs{
		Ok:          ok,
		Id:          mapId,
		ReduceFiles: files,
	}
	reply := ResultReply{}
	call("Master.MapResult", &args, &reply)
}

// CallReduceResult 通知 master Reduce 任务已经完成
func CallReduceResult(ok bool, reduceId int) {
	args := ReduceResultArgs{
		Ok: ok,
		Id: reduceId,
	}
	reply := ResultReply{}
	call("Master.ReduceResult", &args, &reply)
}

// runMapTask 执行 map 任务
func runMapTask(filename string, id int, r int, mapFun func(string, string) []KeyValue) (bool, []string) {
	// 读取 map 传入文件
	data, err := ioutil.ReadFile(filename)
	// fmt.Println(string(data))
	if err != nil {
		return false, nil
	}
	kvs := mapFun(filename, string(data))

	// 创建 map 任务中间文件（mr-{mapId}-{reduceN}.txt）
	files := make([]string, r)
	for i := range files {
		files[i] = fmt.Sprintf("mr-%d-%d.txt", id, i)
	}
	// fmt.Printf("files: %+v \n ", files)

	// 创建 map 任务写入 json encoder 数组
	encodes := make([]*json.Encoder, r)
	fileWriters := make([]*os.File, r)
	// 根据 reduce 任务数量，创建对应的临时文件 Writer
	for i := range encodes {
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
	// 排序 map 结果
	sort.Sort(ByKey(kvs))
	// 缓存 key 的 hash 结果
	var kr int
	var kk string
	for _, kv := range kvs {
		if kv.Key != kk {
			kr = iHash(kv.Key) % r
			kk = kv.Key
		}
		// 根据 iHash 对 key 的处理结果与 reduce 数量取模写入特定文件
		err = encodes[kr].Encode(&kv)
		if err != nil {
			fmt.Println(err)
			return false, nil
		}
	}
	// kv 写入文件完成，关闭临时文件，并使用 Rename 函数将临时文件重命名文件到目标目录
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

// readFile 读取文件并使用 json Decoder 解析成 kv 数组
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
	// 将文件内容解析到 kv 数组
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva, nil
}

// runReduceTask 执行 Reduce 任务
func runReduceTask(id int, filenames string, reduceFun func(string, []string) string) bool {
	// 将文件名列表解析出来
	files := strings.Split(filenames, ",")
	kva := make([]KeyValue, 1000)
	// 逐个文件解析并加入总的 kv 数组中
	for _, f := range files {
		kvs, err := readFile(f)
		if err != nil {
			return false
		}
		kva = append(kva, kvs...)
	}
	// 对 kv 数组排序
	sort.Sort(ByKey(kva))
	// 生成 reduce 输出临时文件
	outFile := fmt.Sprintf("mr-out-%d", id)
	fileWriter, err := ioutil.TempFile("", outFile)
	defer func() {
		_ = fileWriter.Close()
	}()
	if err != nil {
		fmt.Println(err)
		return false
	}
	// 按 key 获取数据组装成 values 数组，传入 reduce 函数中执行并将结果写入文件
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
		// 执行 reduce 函数并将结果写入文件
		output := reduceFun(kva[i].Key, values)
		if kva[i].Key != "" {
			_, err = fmt.Fprintf(fileWriter, "%v %v\n", kva[i].Key, output)
			if err != nil {
				fmt.Println(err)
				return false
			}
		}
		i = j
	}
	// 关闭临时文件并使用 Rename 函数重命名结果到目标文件目录
	if err := fileWriter.Close(); err != nil {
		fmt.Println(err)
	}
	if err := os.Rename(fileWriter.Name(), outFile); err != nil {
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
func call(rpcName string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockName := masterSock()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer func() {
		_ = c.Close()
	}()

	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
