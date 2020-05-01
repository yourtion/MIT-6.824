package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// Master 主进程
type Master struct {
	nReduce     int          // number of reduce tasks to use.
	mapDone     bool         // is all map task done
	reduceDone  bool         // is all reduce task done
	mapFiles    []string     // file for map
	reduceFiles [][]string   // file for reduce
	lock        sync.RWMutex // master data lock
	mapIds      int
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
		reply.MapId = m.mapIds
	} else {
		m.mapDone = true
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
		reduceFiles: make([][]string, nReduce),
		lock:        sync.RWMutex{},
	}
	m.server()
	return &m
}
