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

// Master 主进程
type Master struct {
	nReduce       int               // number of reduce tasks to use.
	mapDone       bool              // is all map task done
	reduceDone    bool              // is all reduce task done
	mapFiles      []string          // file for map
	mappingFiles  map[int]*fileInfo //
	mappedCount   int               // map count
	reduceFiles   [][]string        // file for reduce
	reducingFiles map[int]*fileInfo // file for reduce
	reducedCount  int               // reduce count
	lock          sync.RWMutex      // master data lock
	mapIds        int
	reduceIds     int
}

type fileInfo struct {
	Time time.Time
	File string
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
		reply.TaskType = TaskMap
		reply.Param = file
		reply.ReduceN = m.nReduce
		reply.TaskId = m.mapIds
		m.mappingFiles[m.mapIds] = &fileInfo{Time: time.Now(), File: file}
		return nil
	}

	if m.mapDone && len(m.reduceFiles) > 0 {
		m.reduceIds += 1
		files := m.reduceFiles[0]
		m.reduceFiles = m.reduceFiles[1:]
		reply.TaskType = TaskReduce
		reply.Param = strings.Join(files, ",")
		reply.ReduceN = m.nReduce
		reply.TaskId = m.reduceIds
		m.reducingFiles[m.reduceIds] = &fileInfo{Time: time.Now(), File: reply.Param}
		return nil
	}

	if m.mapDone && m.reduceDone {
		reply.TaskType = TaskExit
		return nil
	}

	return nil
}

func (m *Master) MapResult(args *MapResultArgs, _ *ResultReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if args.Ok {
		m.mappedCount -= 1
		delete(m.mappingFiles, args.Id)
		if m.mappedCount == 0 {
			m.mapDone = true
		}
		// fmt.Printf("%+v\n", args)
		for i, file := range args.ReduceFiles {
			m.reduceFiles[i] = append(m.reduceFiles[i], file)
		}
		// fmt.Printf("reduceFiles : %+v\n", m.reduceFiles)
	} else {
		fmt.Printf("MapResult Error: %+v\n", args)
		f := m.mappingFiles[args.Id]
		if f != nil {
			m.mapFiles = append(m.mapFiles, f.File)
		}
	}
	return nil
}

func (m *Master) ReduceResult(args *ReduceResultArgs, _ *ResultReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	// fmt.Printf("SendReduceResultArgs : %+v \n", args)
	if args.Ok {
		m.reducedCount -= 1
		delete(m.reducingFiles, args.Id)
		if m.reducedCount == 0 {
			m.reduceDone = true
		}
	} else {
		fmt.Printf("ReduceResult Error: %+v\n", args)
		f := m.reducingFiles[args.Id]
		if f != nil {
			m.reduceFiles = append(m.reduceFiles, strings.Split(f.File, ","))
		}
	}
	return nil
}

func (m *Master) watchDog() {
	for {
		func() {
			m.lock.Lock()
			defer m.lock.Unlock()

			if m.mapDone && m.reduceDone {
				return
			}

			now := time.Now()
			for i, f := range m.mappingFiles {
				if now.Sub(f.Time) > 10*time.Second {
					delete(m.mappingFiles, i)
					m.mapFiles = append(m.mapFiles, f.File)
				}
			}
			for i, f := range m.reducingFiles {
				if now.Sub(f.Time) > 10*time.Second {
					delete(m.reducingFiles, i)
					m.reduceFiles = append(m.reduceFiles, strings.Split(f.File, ","))
				}
			}
		}()
		time.Sleep(time.Second)
	}
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
		mappingFiles:  map[int]*fileInfo{},
		mappedCount:   len(files),
		reduceFiles:   make([][]string, nReduce),
		reducingFiles: map[int]*fileInfo{},
		reducedCount:  nReduce,
		lock:          sync.RWMutex{},
	}
	m.server()
	go m.watchDog()
	return &m
}
