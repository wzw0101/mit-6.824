package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

const (
	taskTypeNone   = 0
	taskTypeMap    = 1
	taskTypeReduce = 2
)

type NextTaskArgs struct {
}

type NextTaskReply struct {
	TaskType int // 0: wait, 1: map task, 2: reduce task
	NReduce  int

	TaskId     int
	Inputfname string
}

type NotifyOfTaskCompletionArgs struct {
	TaskType   int
	TaskId     int
	Inputfname string
}

type NotifyOfTaskCompletionReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
