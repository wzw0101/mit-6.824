package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	m                 sync.Mutex
	mapTaskTodo       []string
	mapTaskPending    map[string]int64
	reduceTaskTodo    []int
	reduceTaskPending map[int]int64

	nReduce int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) NextTask(args *NextTaskArgs, reply *NextTaskReply) error {
	c.m.Lock()
	defer c.m.Unlock()

	now := time.Now().Unix()
	if len(c.mapTaskTodo) > 0 {
		reply.TaskType = taskTypeMap
		reply.NReduce = c.nReduce

		reply.Inputfname = c.mapTaskTodo[len(c.mapTaskTodo)-1]
		c.mapTaskTodo = c.mapTaskTodo[:len(c.mapTaskTodo)-1]
		c.mapTaskPending[reply.Inputfname] = now + 10
		return nil
	}

	if (len(c.mapTaskTodo) == 0 && len(c.mapTaskPending) > 0) || len(c.reduceTaskTodo) == 0 {
		reply.TaskType = taskTypeNone
		return nil
	}

	reply.TaskType = taskTypeReduce
	reply.NReduce = c.nReduce
	reply.TaskId = c.reduceTaskTodo[len(c.reduceTaskTodo)-1]
	c.reduceTaskTodo = c.reduceTaskTodo[:len(c.reduceTaskTodo)-1]
	c.reduceTaskPending[reply.TaskId] = now + 10
	return nil
}

func (c *Coordinator) NotifyOfTaskCompletion(args *NotifyOfTaskCompletionArgs, reply *NotifyOfTaskCompletionReply) error {
	c.m.Lock()
	defer c.m.Unlock()

	if args.TaskType == taskTypeMap {
		if c.mapTaskPending[args.Inputfname] != 0 {
			delete(c.mapTaskPending, args.Inputfname)
		}
	} else {
		if c.reduceTaskPending[args.TaskId] != 0 {
			delete(c.reduceTaskPending, args.TaskId)
		}
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.m.Lock()
	defer c.m.Unlock()
	ret = len(c.mapTaskTodo) == 0 && len(c.mapTaskPending) == 0 && len(c.reduceTaskTodo) == 0 && len(c.reduceTaskPending) == 0
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTaskTodo = files
	c.mapTaskPending = make(map[string]int64)
	c.reduceTaskTodo = make([]int, 0, nReduce)
	c.reduceTaskPending = make(map[int]int64)
	for i := 0; i < nReduce; i += 1 {
		c.reduceTaskTodo = append(c.reduceTaskTodo, i)
	}

	c.nReduce = nReduce

	c.server()

	go func() {
		for {
			c.m.Lock()
			now := time.Now().Unix()
			for mapTask, expireAt := range c.mapTaskPending {
				if now > expireAt {
					log.Printf("map task %v timeout", mapTask)
					delete(c.mapTaskPending, mapTask)
					c.mapTaskTodo = append(c.mapTaskTodo, mapTask)
				}
			}
			for reduceTask, expireAt := range c.reduceTaskPending {
				if now > expireAt {
					log.Printf("reduce task %v timeout", reduceTask)
					delete(c.reduceTaskPending, reduceTask)
					c.reduceTaskTodo = append(c.reduceTaskTodo, reduceTask)
				}
			}
			c.m.Unlock()
			time.Sleep(time.Second)
		}
	}()
	return &c
}
