package mr

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type WorkerStatus string
type CoordinatorStatus string
type CompletionStatus string

const (
	INPROGRESS WorkerStatus = "work-in-progress task"
	CRASH      WorkerStatus = "crashed" // CRASH parameterized by TIMEQUANTA
	COMPLETED  WorkerStatus = "task completed"
)

const (
	IDLE          CoordinatorStatus = "idle"
	PENDINGMAP    CoordinatorStatus = "pending map tasks"
	PENDINGREDUCE CoordinatorStatus = "pending reduce tasks"
	DONE          CoordinatorStatus = "done"
)

const (
	UNALLOCATED  CompletionStatus = "some unallocated task" // UNALLOCATED | CRASH -> UNALLOCATED
	PENDINGTASK  CompletionStatus = "all tasks allocated but pending completion"
	COMPLETEDALL CompletionStatus = "all tasks completed"
)

type TaskProgress struct {
	workerID []byte
	sno      int
	status   WorkerStatus
}

type Coordinator struct {
	mu          sync.Mutex
	currStatus  CoordinatorStatus
	nReduceTask int
	files       []string                // files stores the filenames sorted lexicographically
	mapTask     map[string]TaskProgress // key -> c.files[value.sno]
	reduceTask  map[int]TaskProgress    // key -> value.sno
}

func (c *Coordinator) nextMapTask() int {
	for idx, file := range c.files {
		val, present := c.mapTask[file]
		if !present || val.status == CRASH {
			log.Printf("[coordinator] [map] file %v needs to be allocated\n", file)
			return idx
		}
	}
	return -1
}

func (c *Coordinator) nextReduceTask() int {
	for idx := 0; idx < c.nReduceTask; idx++ {
		val, present := c.reduceTask[idx]
		if !present || val.status == CRASH {
			log.Printf("[coordinator] [reduce] file mr-out-%v needs to be allocated\n", idx)
			return idx
		}
	}
	return -1
}

func (c *Coordinator) mapCompleted() CompletionStatus {
	status := COMPLETEDALL
	for _, file := range c.files {
		val, present := c.mapTask[file]
		if !present || val.status == CRASH {
			log.Printf("[coordinator] unallocated file found\n")
			return UNALLOCATED
		}
		if val.status != COMPLETED {
			status = PENDINGTASK
		}
	}
	if status == COMPLETEDALL {
		c.currStatus = PENDINGREDUCE
	}
	log.Printf("[coordinator] [map] completion status: %v\n", c.currStatus)
	return status
}

func (c *Coordinator) reduceCompleted() CompletionStatus {
	status := COMPLETEDALL
	for idx := 0; idx < c.nReduceTask; idx++ {
		val, present := c.reduceTask[idx]
		if !present || val.status == CRASH {
			log.Printf("[coordinator] unallocated file found\n")
			return UNALLOCATED
		}
		if val.status != COMPLETED {
			status = PENDINGTASK
		}
	}
	if status == COMPLETEDALL {
		c.currStatus = DONE
	}
	log.Printf("[coordinator] [reduce] completion status: %v\n", c.currStatus)
	return status
}

// updateMapStatus deletes the entry than changing just the status.
// CRASH || INPROGRESS -> CRASH after timeout
// MAX LIFE TIME: 10 * TIMEQUANTA
func (c *Coordinator) updateMapStatus(sno int) error {
	startTime := time.Now()

	for time.Now().Sub(startTime) < 25*TIMEQUANTA { // FIXME: Set sane parameters
		time.Sleep(TIMEQUANTA)
		c.mu.Lock()
		switch c.mapTask[c.files[sno]].status {
		case CRASH:
			log.Printf("[coordinator] [worker-%v] [map] timeout. status: %v\n", sno, CRASH)
			delete(c.mapTask, c.files[sno])
			c.mu.Unlock()
			return nil
		case COMPLETED:
			c.mapCompleted()
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
	}
	log.Printf("[coordinator] [worker-%v] [map] updating task status\n", sno)
	c.mu.Lock()
	if c.mapTask[c.files[sno]].status != COMPLETED {
		log.Printf("[coordinator] [worker-%v] [map] timeout: %v\n", sno, CRASH)
		delete(c.mapTask, c.files[sno])
	}
	c.mapCompleted()
	c.mu.Unlock()
	return nil
}

// updateReduceStatus deletes the entry than changing just the status.
// CRASH || INPROGRESS -> CRASH after timeout
func (c *Coordinator) updateReduceStatus(sno int) error {
	startTime := time.Now()
	for time.Now().Sub(startTime) < 25*TIMEQUANTA { // FIXME: Set sane parameters
		time.Sleep(TIMEQUANTA)
		c.mu.Lock()
		switch c.reduceTask[sno].status {
		case CRASH:
			log.Printf("[coordinator] [worker-%v] [reduce] timeout. status: %v\n", sno, CRASH)
			delete(c.reduceTask, sno)
			c.mu.Unlock()
			return nil
		case COMPLETED:
			c.reduceCompleted()
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
	}

	log.Printf("[coordinator] [worker-%v] [reduce] updating task status\n", sno)
	c.mu.Lock()
	if c.reduceTask[sno].status != COMPLETED {
		log.Printf("[coordinator] [worker-%v] [reduce] timeout: %v\n", sno, CRASH)
		delete(c.reduceTask, sno)
	}
	c.reduceCompleted()
	c.mu.Unlock()
	return nil
}

// RegisterWorker
// Lock only places where data race can occur, i.e. make them atomic
// Where should we update the status of our Coordinator?
func (c *Coordinator) RegisterWorker(args *GetTaskArgs, reply *GetTaskReply) error {
	var taskIdx int

	c.mu.Lock()
	status := c.currStatus
	log.Printf("[coordinator] current status: %v\n", status)
	switch status {
	case IDLE:
		log.Print("[coordinator] [map] init\n")
		c.currStatus = PENDINGMAP
		fallthrough
	case PENDINGMAP: // either some task or NONE
		mapStatus := c.mapCompleted()
		log.Printf("[coordinator] [map] %v\n", mapStatus)
		switch mapStatus {
		case UNALLOCATED:
			taskIdx = c.nextMapTask()
			if taskIdx == -1 { // since unallocated, atleast one task should be there
				c.mu.Unlock()
				log.Printf("[coordinator] [map] unallocated task exists but not found\n")
				return errors.New("[coordinator] [map] unallocated task exists but not found\n")
			}
			c.mapTask[c.files[taskIdx]] = TaskProgress{
				workerID: args.WorkerID,
				sno:      taskIdx,
				status:   INPROGRESS,
			}
			c.mu.Unlock()
			// If some task crashed, delete its entry from the map
			go c.updateMapStatus(taskIdx)
			reply.Task = MAP
			reply.SerialNo = taskIdx
			reply.NReduceTask = c.nReduceTask
			reply.NMapTask = len(c.files)
			reply.FileName = c.files[taskIdx]
			return nil
		case PENDINGTASK: // waiting for completion of all allocated tasks
			c.mu.Unlock()
			reply.Task = WAITING
			return nil
		case COMPLETEDALL: // all tasks are completed
			log.Fatalf("[coordinator] [map] all tasks completed but status not updated yet\n")
		}
	case PENDINGREDUCE: // either some task or NONE
		reduceStatus := c.reduceCompleted()
		log.Printf("[coordinator] [reduce] %v\n", reduceStatus)
		switch reduceStatus {
		case UNALLOCATED:
			taskIdx = c.nextReduceTask()
			if taskIdx == -1 { // since unallocated, atleast one task should be there
				c.mu.Unlock()
				log.Printf("[coordinator] [reduce] unallocated task exists but not found\n")
				return errors.New("[coordinator] [reduce] unallocated task exists but not found\n")
			}
			c.reduceTask[taskIdx] = TaskProgress{
				workerID: args.WorkerID,
				sno:      taskIdx,
				status:   INPROGRESS,
			}
			c.mu.Unlock()
			go c.updateReduceStatus(taskIdx)
			reply.Task = REDUCE
			reply.SerialNo = taskIdx
			reply.NReduceTask = c.nReduceTask
			reply.NMapTask = len(c.files)
			reply.FileName = fmt.Sprintf("mr-out-%v", taskIdx)
			return nil
		case PENDINGTASK:
			c.mu.Unlock()
			reply.Task = WAITING
			return nil
		default:
			c.mu.Unlock()
			log.Fatalf("[coordinator] [reduce] unreachable code\n")
		}
	case DONE:
		c.mu.Unlock()
		log.Print("[coordinator] all tasks completed\n")
		reply.Task = ALLDONE
		return nil
	}
	log.Fatalf("[coordinator] [RegisterWorker] unreachable code\n")
	return errors.New("literally unreachable code. below a fatalf but golang won't fucking shutup")
}

// WorkerACK Lock only places where data race can occur, i.e. make them atomic
// # Optimization:
//   - Worker can send FAIL. Update the status right after. No need for waiting.
func (c *Coordinator) WorkerACK(args *PutTaskArgs, reply *PutTaskReply) error {
	log.Printf("[coordinator] [worker-%v] SYN received \n", args.Sno)
	switch args.Task {
	case WAITING:
		log.Printf("[coordinator] [SYN] malformed worker with no task\n")
		reply.ACK = false
		return errors.New("[coordinator] worker without task can't ACK\n")
	case MAP:
		taskNo := args.Sno
		c.mu.Lock()
		processedFile := c.files[taskNo]
		workerDetails, exists := c.mapTask[processedFile]
		c.mu.Unlock()
		// confirm existence of worker
		if !exists {
			log.Printf("[coordinator] [map] [worker-%d] [SYN] malformed sno\n", taskNo)
			reply.ACK = false
			return errors.New("[worker] [SYN] malformed sno\n")
		}

		// 		* Confirm workerID
		if string(workerDetails.workerID) != string(args.WorkerID) {
			log.Printf("[coordinator] [map] [SYN] task not assigned to worker with id #%v\n", args.WorkerID)
			reply.ACK = false
			return errors.New("[coordinator] [map] [SYN] task not assigned to the worker\n")
		}

		// check if all files are accessible.
		var f *os.File
		var err error
		for idx := 0; idx < c.nReduceTask; idx++ {
			file := fmt.Sprintf("mr-intermediate-%d-%d", taskNo, idx)
			if f, err = os.Open(file); err != nil {
				log.Printf("[coordinator] [worker-%d] [map] [SYN] open: intermediate file not accessible\n", taskNo)
				// change STATUS to CRASH. redo the test
				c.mu.Lock()
				c.mapTask[processedFile] = TaskProgress{
					workerID: workerDetails.workerID,
					sno:      workerDetails.sno,
					status:   CRASH,
				}
				c.mu.Unlock()
				reply.ACK = false
				return err
			}
			err = f.Close()
			if err != nil {
				log.Printf("[coordinator] [worker-%d] [map] [SYN] close: intermediate file not accessible\n", taskNo)
				c.mu.Lock()
				c.mapTask[processedFile] = TaskProgress{
					workerID: workerDetails.workerID,
					sno:      workerDetails.sno,
					status:   CRASH,
				}
				c.mu.Unlock()
				reply.ACK = false
				return err
			}
		}

		// Update status
		c.mu.Lock()
		c.mapTask[processedFile] = TaskProgress{
			workerID: workerDetails.workerID,
			sno:      workerDetails.sno,
			status:   COMPLETED,
		}
		c.mu.Unlock()
		c.mapCompleted()
		log.Printf("[coordinator] [worker-%v] [map] task COMPLETED\n", taskNo)

	case REDUCE:
		taskNo := args.Sno
		c.mu.Lock()
		workerDetails, exists := c.reduceTask[taskNo]
		nMapWorkers := len(c.files)
		c.mu.Unlock()

		// confirm existence of registeredWorker
		if !exists {
			log.Printf("[coordinator] [worker-%d] [reduce] malformed sno\n", taskNo)
			reply.ACK = false
			return errors.New("[worker] [reduce] malformed sno\n")
		}

		// 		* Confirm workerID
		if string(workerDetails.workerID) != string(args.WorkerID) {
			log.Printf("[coordinator] [reduce] task not assigned to worker with id #%v\n", args.WorkerID)
			reply.ACK = false
			return errors.New("[coordinator] [reduce] task not assigned to the worker\n")
		}

		var f *os.File
		var err error
		for idx := 0; idx < nMapWorkers; idx++ {
			fileName := fmt.Sprintf("mr-intermediate-%v-%v", idx, taskNo)
			log.Printf("[coordinator] [reduce] file: %v", fileName)
			if f, err = os.Open(fileName); err != nil {
				log.Printf("[coordinator] [worker-%d] [reduce] open: intermediate file not accessible\n", taskNo)
				// change STATUS to CRASH. Re-do the test
				c.mu.Lock()
				c.reduceTask[taskNo] = TaskProgress{
					workerID: workerDetails.workerID,
					sno:      workerDetails.sno,
					status:   CRASH,
				}
				c.mu.Unlock()
				reply.ACK = false
				return err
			}
			err = f.Close()
			if err != nil {
				log.Printf("[coordinator] [worker-%d] [reduce] close: intermediate file not accessible\n", taskNo)
				c.mu.Lock()
				c.reduceTask[taskNo] = TaskProgress{
					workerID: workerDetails.workerID,
					sno:      workerDetails.sno,
					status:   CRASH,
				}
				c.mu.Unlock()
				reply.ACK = false
				return err
			}
		}
		// Update status
		c.mu.Lock()
		c.reduceTask[taskNo] = TaskProgress{
			workerID: workerDetails.workerID,
			sno:      workerDetails.sno,
			status:   COMPLETED,
		}
		log.Printf("[coordinator] [reduce] [worker-%v] task COMPLETED\n", taskNo)
		c.reduceCompleted()
		c.mu.Unlock()

	case ALLDONE:
		log.Printf("[coordinator] all tasks already done\n")
		log.Printf("[coordinator] abnormal SYN\n")
		reply.ACK = false
		return errors.New("[worker] abnormal SYN, all tasks already done\n")
	default:
		log.Printf("[coordinator] malformed task: %v\n", args.Task)
		reply.ACK = false
		return errors.New(fmt.Sprintf("malformed task %v\n", args.Task))
	}
	reply.ACK = true
	log.Printf("[coordinator] ACK sent\n")
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	err := os.Remove(sockname)
	if err != nil {
		log.Fatal("[sock] permission error:", err)
	}
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) MapProgress() float32 {
	completedMapTasks := 0
	c.mu.Lock()
	for _, val := range c.mapTask {
		if val.status == COMPLETED {
			completedMapTasks++
		}
	}
	c.mu.Unlock()
	progress := float32(completedMapTasks / len(c.files))
	log.Printf("[coordinator] [map] progress: %v\n", progress)
	return progress
}

func (c *Coordinator) ReduceProgress() float32 {
	completedReduceTasks := 0
	c.mu.Lock()
	for _, val := range c.reduceTask {
		if val.status == COMPLETED {
			completedReduceTasks++
		}
	}
	c.mu.Unlock()
	progress := float32(completedReduceTasks / c.nReduceTask)
	log.Printf("[coordinator] [reduce] progress: %v\n", progress)
	return progress
}

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	log.Printf("[coordinator] status: %v\n", c.currStatus)
	if c.currStatus == DONE {
		return true
	}
	c.mu.Unlock()
	return false
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
/*
1. [x] Fill the Coordinator with corresponding fields (files and numTasks)
2. [x] Set up the http servers for workers to connect with.
3. [x] Setup RPC handlers.
  - Change coordinator state together with it
*/
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Print("[coordinator] [init]\n")
	sort.Sort(ByKey(files)) // specifying file order
	c := Coordinator{
		files:       files,
		currStatus:  IDLE,
		nReduceTask: nReduce,
		mapTask:     make(map[string]TaskProgress),
		reduceTask:  make(map[int]TaskProgress),
	}
	c.server()
	return &c
}
