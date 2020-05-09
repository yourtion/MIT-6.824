package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

// 任务超时时间（10s）
const configTimeOut = 10 * time.Second

// Master 主进程
type Master struct {
	nReduce       int              // reduce 任务数量
	mapDone       bool             // 所有 Map 任务已经完成
	reduceDone    bool             // 所有 Reduce 任务已经完成
	mapFiles      []string         // 需要执行 Map 任务的文件列表（启动Master传入）
	mappingFiles  map[int]fileInfo // 正在执行的 Map 任务，Key 为 Map WorkerId
	mappedCount   int              // 已完成 Map 的任务数量
	reduceFiles   [][]string       // 需要执行 Reduce 任务的文件列表（Map任务完成后回调）
	reducingFiles map[int]fileInfo // 正在执行的 Reduce 任务，Key 为 Reduce WorkerId
	reducedCount  int              // 已完成 Reduce 的任务数量
	lock          sync.RWMutex     // Master 数据锁
	mapId         int              // Map 任务计数器，每次启动 Map 任务递增
	reduceId      int              // Reduce 任务计数器，每次启动 Reduce 任务递增
}

// fileInfo 任务文件信息
type fileInfo struct {
	Time time.Time // 任务启动时间
	File string    // 任务相关文件
}

// Example is example RPC handler. the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// GetTask 获取任务（先执行Map，再执行Reduce，完成后触发Exit）
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	DLog("GetTask From : %d \n", args.WorkerId)
	now := time.Now()

	// 检查 Map 任务超时
	if !m.mapDone {
		for i, f := range m.mappingFiles {
			if now.Sub(f.Time) < configTimeOut {
				continue
			}
			delete(m.mappingFiles, i)
			m.mapFiles = append(m.mapFiles, f.File)
			DLog("Timeout mappingFiles : %d -> %s \n", i, f.File)
		}
	}
	// 检查 Reduce 任务超时
	if !m.reduceDone {
		for i, f := range m.reducingFiles {
			if now.Sub(f.Time) < configTimeOut {
				continue
			}
			delete(m.reducingFiles, i)
			m.reduceFiles = append(m.reduceFiles, strings.Split(f.File, ","))
			DLog("Timeout reduceFiles : %d -> %s \n", i, f.File)
		}
	}

	// 如果有 Map 任务返回 Map 任务信息
	if len(m.mapFiles) > 0 {
		m.mapId += 1
		file := m.mapFiles[0]
		m.mapFiles = m.mapFiles[1:]
		reply.TaskType = TaskMap
		reply.Param = file
		reply.ReduceN = m.nReduce
		reply.TaskId = m.mapId
		m.mappingFiles[m.mapId] = fileInfo{Time: now, File: file}
		return nil
	}

	// 如果有 Map 任务已经完成，同时还有 Reduce 任务返回 Reduce 任务信息
	if m.mapDone && len(m.reduceFiles) > 0 {
		m.reduceId += 1
		files := m.reduceFiles[0]
		m.reduceFiles = m.reduceFiles[1:]
		reply.TaskType = TaskReduce
		reply.Param = strings.Join(files, ",")
		reply.ReduceN = m.nReduce
		reply.TaskId = m.reduceId
		m.reducingFiles[m.reduceId] = fileInfo{Time: now, File: reply.Param}
		return nil
	}

	// 任务都已经完成，返回 Exit 让 Worker 退出
	if m.mapDone && m.reduceDone {
		reply.TaskType = TaskExit
		return nil
	}

	return nil
}

// MapResult 执行 Map 任务完成回调
func (m *Master) MapResult(args *MapResultArgs, _ *ResultReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	DLog("MapResult：%+v\n", args)
	// 如果任务成功执行则并将结果放入 ReduceFiles 中，待完成 Map 数量减一，否则回滚文件
	if args.Ok {
		m.mappedCount -= 1
		if m.mappedCount == 0 {
			m.mapDone = true
		}
		for i, file := range args.ReduceFiles {
			m.reduceFiles[i] = append(m.reduceFiles[i], file)
		}
		DLog("reduceFiles : %+v\n", m.reduceFiles)
	} else {
		fmt.Printf("MapResult Error: %+v\n", args)
		f, ok := m.mappingFiles[args.Id]
		if ok {
			m.mapFiles = append(m.mapFiles, f.File)
		}
	}
	// 删除执行中信息
	delete(m.mappingFiles, args.Id)
	return nil
}

// ReduceResult 执行 Reduce 任务完成回调
func (m *Master) ReduceResult(args *ReduceResultArgs, _ *ResultReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	DLog("ReduceResult：%+v\n", args)
	// 如果任务成功执行则待完成 Reduce 数量减一，否则回滚文件
	if args.Ok {
		m.reducedCount -= 1
		if m.reducedCount == 0 {
			m.reduceDone = true
		}
	} else {
		fmt.Printf("ReduceResult Error: %+v\n", args)
		f, ok := m.reducingFiles[args.Id]
		if ok {
			m.reduceFiles = append(m.reduceFiles, strings.Split(f.File, ","))
		}
	}
	// 删除执行中信息
	delete(m.reducingFiles, args.Id)
	return nil
}

// server start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	_ = rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockName := masterSock()
	_ = os.Remove(sockName)
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		_ = http.Serve(l, nil)
	}()
}

// Done main/mrmaster.go calls Done() periodically to find out if the entire job has finished.
func (m *Master) Done() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.mapDone && m.reduceDone
}

// MakeMaster create a Master. main/mrmaster.go calls this function. nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	var m = Master{
		nReduce:       nReduce,
		mapDone:       false,
		reduceDone:    false,
		mapFiles:      files,
		mappingFiles:  map[int]fileInfo{},
		mappedCount:   len(files),
		reduceFiles:   make([][]string, nReduce),
		reducingFiles: map[int]fileInfo{},
		reducedCount:  nReduce,
		lock:          sync.RWMutex{},
	}
	// 启动服务
	m.server()
	return &m
}
