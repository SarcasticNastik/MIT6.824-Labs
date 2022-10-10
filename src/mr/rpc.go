package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type WorkerType string

const (
	WAITING WorkerType = "pending task completion"
	ALLDONE WorkerType = "mapreduce tasks completed"
	MAP     WorkerType = "map"
	REDUCE  WorkerType = "reduce"
)

const TIMEQUANTA = 300 * time.Millisecond

// ByKey for sorting by key.
type ByKey []string

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i] < a[j] }

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type GetTaskArgs struct {
	WorkerID []byte
}

type GetTaskReply struct {
	SerialNo    int // if reduce Task, SerialNo gives the output Task number. ignore FileName
	Task        WorkerType
	FileName    string // if map Task, FileName main. ignore SerialNo
	NReduceTask int
	NMapTask    int
}

type PutTaskArgs struct {
	WorkerID []byte
	Task     WorkerType
	Sno      int
}

type PutTaskReply struct {
	ACK bool
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
