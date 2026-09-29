package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// 1 - pending
type Task struct {
	TaskId            int64
	Status            uint8 // 0 idle, 1 processing, 2 done
	FilePath          string
	StartProcessingAt time.Time // -1 mean not processing
	BucketId          int
}

type Coordinator struct {
	MapTask       []Task
	ReduceTask    []Task
	maxId         int64
	NReducer      int
	IsReducePhase bool
	MtLock        sync.Mutex
}

func (c *Coordinator) GetTask(req *GetTaskRequest, reply *GetTaskResponse) error {
	c.MtLock.Lock()
	defer c.MtLock.Unlock()
	if c.isDone() {
		reply.TaskId = "-1"
		return nil
	}

	if !c.IsReducePhase {
		for id, t := range c.MapTask {
			if t.Status == 2 {
				continue
			}
			if t.Status == 1 && t.tooLong() {
				c.MapTask[id].StartProcessingAt = time.Now()
				reply.TaskId = strconv.FormatInt(t.TaskId, 10)
				reply.IsMapWorker = true
				reply.InputFileName = t.FilePath
				reply.NReduce = c.NReducer
				return nil
			}

			if t.Status == 0 {
				c.MapTask[id].Status = 1
				c.MapTask[id].StartProcessingAt = time.Now()
				reply.TaskId = strconv.FormatInt(t.TaskId, 10)
				reply.IsMapWorker = true
				reply.InputFileName = t.FilePath
				reply.NReduce = c.NReducer
				return nil
			}
		}
	} else {
		for id, t := range c.ReduceTask {
			if t.Status == 2 {
				continue
			}

			if t.Status == 1 && t.tooLong() {
				c.ReduceTask[id].StartProcessingAt = time.Now()
				reply.TaskId = strconv.FormatInt(t.TaskId, 10)
				reply.IsMapWorker = false
				reply.BucketId = t.BucketId
				reply.NReduce = c.NReducer
				return nil
			}

			if t.Status == 0 {
				c.ReduceTask[id].Status = 1
				c.ReduceTask[id].StartProcessingAt = time.Now()
				reply.TaskId = strconv.FormatInt(t.TaskId, 10)
				reply.IsMapWorker = false
				reply.BucketId = t.BucketId
				reply.NReduce = c.NReducer
				return nil
			}
		}
	}

	reply.TaskId = "-2"
	return nil
}

func (c *Coordinator) MarkDone(req *MarkTaskDoneRequest, reply *MarkTaskDoneResponse) error {
	c.MtLock.Lock()
	defer c.MtLock.Unlock()
	if req.IsMapTask {
		phaseDone := true
		for id, t := range c.MapTask {
			if req.TaskId == t.TaskId {
				c.MapTask[id].Status = 2
			}
			if c.MapTask[id].Status != 2 {
				phaseDone = false
			}
		}
		if phaseDone {
			c.IsReducePhase = true
		}
	} else {
		for id, t := range c.ReduceTask {
			if req.TaskId == t.TaskId {
				c.ReduceTask[id].Status = 2
				break
			}
		}
	}
	reply = &MarkTaskDoneResponse{}
	return nil
}

// check if a task is too late
func (c *Task) tooLong() bool {
	now := time.Now()
	return now.Sub(c.StartProcessingAt).Seconds() > 10
}

func (c *Coordinator) init(files []string, nReducer int) {
	for _, file := range files {
		c.maxId++
		c.MapTask = append(c.MapTask, Task{
			TaskId:   c.maxId,
			Status:   0,
			FilePath: file,
		})
	}

	for i := 0; i < nReducer; i++ {
		c.maxId++
		c.ReduceTask = append(c.ReduceTask, Task{
			TaskId:   c.maxId,
			Status:   0,
			BucketId: i,
		})
	}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	c.MtLock.Lock()
	defer c.MtLock.Unlock()
	return c.isDone()
}

func (c *Coordinator) isDone() bool {
	if !c.IsReducePhase {
		return false
	}

	for _, elem := range c.ReduceTask {
		if elem.Status != 2 {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReducer: nReduce,
	}
	c.init(files, nReduce)
	// Your code here.

	c.server(sockname)
	return &c
}
