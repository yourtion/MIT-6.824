package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

// 是否打开 debug 日志，输出调试信息
const isDebug = false

// DLog 打印 debug 日志
func DLog(s string, a ...interface{}) {
	if isDebug {
		fmt.Printf(s, a...)
	}
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// TaskType 任务类型
type TaskType uint8

const (
	TaskExit   TaskType = 0 // 退出 Worker
	TaskMap    TaskType = 1 // Map 任务
	TaskReduce TaskType = 2 // Reduce 任务
)

// GetTaskArgs 获取 Task 参数
type GetTaskArgs struct {
	WorkerId int // Worker Id
}

// GetTaskReply 获取 Task 返回
type GetTaskReply struct {
	TaskType TaskType // 任务类型
	TaskId   int      // 任务 Id
	ReduceN  int      // reduce 数量
	Param    string   // 任务数据
}

// ResultReply 执行结果上报返回
type ResultReply struct {
	Ok bool
}

// MapResultArgs Map 任务执行结果
type MapResultArgs struct {
	Ok          bool     // 是否完成
	Id          int      // Task Id
	ReduceFiles []string // 生成的 Reduce 任务文件列表
}

// ReduceResultArgs Reduce 任务执行结果
type ReduceResultArgs struct {
	Ok bool // 是否完成
	Id int  // Task Id
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
