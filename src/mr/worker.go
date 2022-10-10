package mr

import (
	crand "crypto/rand"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		waitingTime := 0 * time.Millisecond
	START:
		reply, id, ok := GetEmployed()

		if !ok {
			log.Fatalf("[worker] exiting\n")
		}

		task := reply.Task
		sno := reply.SerialNo
		nReduceTask := reply.NReduceTask
		nMapTask := reply.NMapTask
		fileName := reply.FileName

		switch task {
		case WAITING:
			log.Printf("[worker] no task allocated!\n")
			if waitingTime > 100*TIMEQUANTA {
				log.Print("[worker] coordinator timeout\n")
				log.Fatalf("[worker] exiting\n")
			}
			waitingTime += TIMEQUANTA
			log.Printf("[worker] retrying in %v milliseconds...\n", TIMEQUANTA/time.Millisecond)
			time.Sleep(TIMEQUANTA)
			goto START
		case MAP:
			ok = RunMapTask(mapf, fileName, sno, nReduceTask)
		case REDUCE:
			ok = RunReduceTask(reducef, fileName, sno, nMapTask)
		case ALLDONE:
			log.Printf("[worker] no task left\n")
			log.Printf("[worker] exiting\n")
			return
		}

		// MAP/ REDUCE local computation
		if !ok {
			log.Printf("[worker-%v] task failed!\n", reply.SerialNo)
			log.Fatalf("[worker-%v] exiting\n", reply.SerialNo)
			// Send failed task to coordinator
		}
		ackTime := 0 * TIMEQUANTA
	SYN:
		ok = TaskACK(id, task, sno)

		if !ok {
			log.Printf("[worker] can't receive ACK from coordinator\n")
			if ackTime > 100*TIMEQUANTA {
				log.Fatalf("[worker] exiting\n")
			}
			log.Printf("[worker] [SYN] retrying in %v milliseconds...\n", TIMEQUANTA/time.Millisecond)
			ackTime += TIMEQUANTA
			time.Sleep(TIMEQUANTA)
			goto SYN
		}
	}
}

func TaskACK(id []byte, task WorkerType, sno int) bool {
	args := PutTaskArgs{WorkerID: id, Task: task, Sno: sno}
	reply := PutTaskReply{}

	ok := call("Coordinator.WorkerACK", &args, &reply)

	if ok && reply.ACK {
		log.Printf("[worker-%v] [task] coordinator ACK received\n", sno)
		return true
	}
	return false
}

// RunMapTask process
func RunMapTask(mapf func(string, string) []KeyValue, fileName string, sno int, nReduceTask int) bool {
	log.Printf("[worker-%v] [file-%v] [map] init\n", sno, fileName)
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("[worker-%v] cannot open %v\n", sno, fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("[worker-%v] cannot read %v\n", sno, fileName)
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("[worker-%v] cannot close %v\n", sno, fileName)
	}
	kva := mapf(fileName, string(content))

	// Check this keyValue for each key rather than file as a whole dumb-me
	ofile := make([]*os.File, nReduceTask)
	for reduceWorkerIdx := 0; reduceWorkerIdx < nReduceTask; reduceWorkerIdx++ {
		oname := fmt.Sprintf("mr-intermediate-%v-%v", sno, reduceWorkerIdx)
		ofile[reduceWorkerIdx], err = os.OpenFile(oname, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("[worker-%v] [map] can't create file. exiting...", sno)
		}
		defer ofile[reduceWorkerIdx].Close()
	}

	for _, keyValue := range kva {
		_, err = fmt.Fprintf(ofile[ihash(keyValue.Key)%nReduceTask], "%v %v\n", keyValue.Key, keyValue.Value)
		if err != nil {
			log.Fatalf("[worker-%v] [map] cannot modify intermediate file\n", sno)
		}
	}

	log.Printf("[worker-%v] [map] completed\n", sno)
	return true
}

// RunReduceTask
func RunReduceTask(reducef func(string, []string) string, fileName string, sno int, nMapTasks int) bool {
	log.Printf("[worker-%v] [file-%v] [reduce] init\n", sno, fileName)
	mp := make(map[string][]string)
	keys := make([]string, 0)

	for mapWorkerIdx := 0; mapWorkerIdx < nMapTasks; mapWorkerIdx++ {
		intermediateFile := fmt.Sprintf("mr-intermediate-%v-%v", mapWorkerIdx, sno)
		file, err := os.Open(intermediateFile)
		if err != nil {
			log.Fatalf("[worker-%v] cannot open %v\n", sno, intermediateFile)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("[worker-%v] cannot read %v\n", sno, intermediateFile)
		}
		err = file.Close()
		if err != nil {
			log.Fatalf("[worker-%v] cannot close %v\n", sno, intermediateFile)
		}

		kvLines := strings.Split(string(content), "\n")
		kvLines = kvLines[0 : len(kvLines)-1]

		for _, val := range kvLines {
			val := strings.Split(val, " ")
			if len(val) != 2 {
				log.Fatalf("[worker-%v] [reduce] malformed file %v contents\n", sno, intermediateFile)
			}
			value, present := mp[val[0]]
			if !present {
				mp[val[0]] = []string{val[1]}
				keys = append(keys, val[0])
			} else {
				mp[val[0]] = append(value, val[1])
			}
		}
	}
	sort.Sort(ByKey(keys))
	oname := fmt.Sprintf("mr-out-%v", sno)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	for _, key := range keys {
		value := mp[key]
		kva := reducef(fileName, value)

		_, err := fmt.Fprintf(ofile, "%v %v\n", key, kva)
		if err != nil {
			log.Fatalf("[worker-%v] cannot modify final file\n", sno)
		}
	}
	log.Printf("[worker-%v] [reduce] completed\n", sno)
	return true
}

func GetEmployed() (GetTaskReply, []byte, bool) {
	//id := strconv.Itoa(os.Getuid()) + strconv.Itoa(os.Getpid())
	id, _ := crand.Int(crand.Reader, big.NewInt(1e18))
	args := GetTaskArgs{WorkerID: id.Bytes()}
	reply := GetTaskReply{}

	ok := call("Coordinator.RegisterWorker", &args, &reply)

	success := false
	if ok {
		log.Printf("[worker-%v] [file] %s allocated task %v\n", reply.SerialNo, reply.Task, reply.FileName)
		success = true
	} else {
		log.Printf("[worker] call failed!")
	}

	return reply, id.Bytes(), success
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
