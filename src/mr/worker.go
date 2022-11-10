package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task := callNextTask()
		if task == nil {
			break
		}

		var err error
		if task.TaskType == taskTypeNone {
			time.Sleep(time.Second)
			continue
		} else if task.TaskType == taskTypeMap {
			err = handleMapTask(mapf, task)
		} else {
			err = handleReduceTask(reducef, task)
		}

		if err == nil {
			callNotifyOfTaskCompletion(task)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func handleMapTask(mapf func(string, string) []KeyValue, task *NextTaskReply) error {
	fcontent, err := os.ReadFile(task.Inputfname)
	if err != nil {
		log.Printf("fail to open file %v: %v", task.Inputfname, err)
		return err
	}

	kvs := mapf(task.Inputfname, string(fcontent))
	m := make(map[int][]KeyValue)
	for _, kv := range kvs {
		i := ihash(kv.Key) % task.NReduce
		if m[i] == nil {
			m[i] = make([]KeyValue, 0)
		}
		m[i] = append(m[i], kv)
	}

	mapId := ihash(string(fcontent))
	for i, subkvs := range m {
		outputfname := fmt.Sprintf("mr-mapId%v-%v", mapId, i)
		pf, err := os.CreateTemp(".", fmt.Sprintf("%v-*", outputfname))
		if err != nil {
			log.Printf("unable to create tmp file: %v", err)
			return err
		}
		enc := json.NewEncoder(pf)
		err = enc.Encode(subkvs)
		if err != nil {
			log.Printf("encode json error: %v", err)
			return err
		}
		err = pf.Close()
		if err != nil {
			log.Printf("close file error: %v", err)
			return err
		}

		err = os.Rename(pf.Name(), outputfname)
		if err != nil {
			log.Printf("rename file error: %v", err)
			return err
		}
	}
	return nil
}

func handleReduceTask(reducef func(string, []string) string, task *NextTaskReply) error {
	pattern := fmt.Sprintf("mr-mapId*-%v", task.TaskId)
	matches, _ := filepath.Glob(pattern)

	intermediate := make([]KeyValue, 0)
	for _, match := range matches {
		pf, err := os.Open(match)
		if err != nil {
			log.Printf("open file error: %v", err)
			return err
		}

		dec := json.NewDecoder(pf)
		var kva []KeyValue
		err = dec.Decode(&kva)
		if err != nil {
			log.Printf("decode from file error: %v", err)
			return err
		}

		err = pf.Close()
		if err != nil {
			log.Printf("close file error: %v", err)
			return err
		}
		intermediate = append(intermediate, kva...)
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", task.TaskId)
	tmpname := fmt.Sprintf("%v-*", oname)
	pf, err := os.CreateTemp(".", tmpname)
	if err != nil {
		log.Printf("create tmp file error: %v", err)
		return err
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(pf, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	err = pf.Close()
	if err != nil {
		log.Printf("close file error: %v", err)
		return err
	}

	err = os.Rename(pf.Name(), oname)
	if err != nil {
		log.Printf("rename file error: %v", err)
		return err
	}
	return nil
}

func callNextTask() *NextTaskReply {
	args := NextTaskArgs{}
	reply := NextTaskReply{}

	ok := call("Coordinator.NextTask", &args, &reply)
	if !ok {
		return nil
	}
	return &reply
}

func callNotifyOfTaskCompletion(task *NextTaskReply) {
	args := NotifyOfTaskCompletionArgs{
		task.TaskType,
		task.TaskId,
		task.Inputfname,
	}
	reply := NotifyOfTaskCompletionReply{}
	_ = call("Coordinator.NotifyOfTaskCompletion", &args, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
