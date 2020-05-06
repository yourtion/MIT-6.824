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
)

// Master 主进程
type Master struct {
	nReduce     int          // number of reduce tasks to use.
	mapDone     bool         // is all map task done
	reduceDone  bool         // is all reduce task done
	mapFiles    []string     // file for map
	mapCount    int          // map count
	reduceFiles [][]string   // file for reduce
	reduceCount int          // reduce count
	lock        sync.RWMutex // master data lock
	mapIds      int
	reduceIds   int
}

// Example is example RPC handler. the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetTask(_ *GetTaskArgs, reply *GetTaskReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if len(m.mapFiles) > 0 {
		m.mapIds += 1
		file := m.mapFiles[0]
		m.mapFiles = m.mapFiles[1:]
		reply.TaskType = "Map"
		reply.Key = file
		reply.ReduceN = m.nReduce
		reply.TaskId = m.mapIds
		return nil
	}

	if m.mapDone && len(m.reduceFiles) > 0 {
		m.reduceIds += 1
		files := m.reduceFiles[0]
		m.reduceFiles = m.reduceFiles[1:]
		reply.TaskType = "Reduce"
		reply.Key = strings.Join(files, ",")
		reply.ReduceN = m.nReduce
		reply.TaskId = m.reduceIds
		return nil
	}

	if m.mapDone && m.reduceDone {
		reply.TaskType = "Exit"
		return nil
	}

	return nil
}

func (m *Master) SendMapResult(args *SendMapResultArgs, _ *SendMapResultReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if args.Ok {
		m.mapCount -= 1
		if m.mapCount == 0 {
			m.mapDone = true
		}
		// fmt.Printf("%+v\n", args)
		for i, file := range args.ReduceFiles {
			m.reduceFiles[i] = append(m.reduceFiles[i], file)
		}
		// fmt.Printf("reduceFiles : %+v\n", m.reduceFiles)
	} else {
		fmt.Printf("MapResult Error: %+v\n", args)
		m.mapFiles = append(m.mapFiles, args.MapFile)
	}
	return nil
}

func (m *Master) SendReduceResult(args *SendReduceResultArgs, _ *SendReduceResultReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	// fmt.Printf("SendReduceResultArgs : %+v \n", args)
	if args.Ok {
		m.reduceCount -= 1
		if m.reduceCount == 0 {
			m.reduceDone = true
		}
	} else {
		fmt.Printf("ReduceResult Error: %+v\n" , args)
		m.reduceFiles = append(m.reduceFiles, args.Files)
	}
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
		nReduce:     nReduce,
		mapDone:     false,
		reduceDone:  false,
		mapFiles:    files,
		mapCount:    len(files),
		reduceFiles: make([][]string, nReduce),
		reduceCount: nReduce,
		lock:        sync.RWMutex{},
	}
	m.server()
	return &m
}
