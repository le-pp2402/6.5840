package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

func readFile(filename string) []byte {
	file, err := os.Open(filename)

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := io.ReadAll(file)

	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	return content
}

// main/mrworker.go calls this function.
func Worker(
	sockname string,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	for {
		task := CallGetTask()

		if task.TaskId == "-1" || task.TaskId == "" {
			break
		}

		if task.TaskId == "-2" {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if task.IsMapWorker {
			filename := task.InputFileName

			content := readFile(filename)
			res := mapf(filename, string(content))

			buckets := make([][]KeyValue, task.NReduce)

			for _, kv := range res {
				hashedKey := ihash(kv.Key) % task.NReduce
				buckets[hashedKey] = append(buckets[hashedKey], kv)
			}

			for id, elem := range buckets {
				tmpFile, err := os.CreateTemp(".", "mr-*")

				if err != nil {
					log.Fatal("err: " + err.Error())
				}

				enc := json.NewEncoder(tmpFile)

				for _, v := range elem {
					err := enc.Encode(v)

					if err != nil {
						tmpFile.Close()
						log.Fatalln("err: " + err.Error())
					}
				}

				tmpFile.Close()
				outFilename := "mr-" + task.TaskId + "-" + strconv.FormatInt(int64(id), 10)
				os.Rename(tmpFile.Name(), outFilename)
			}

			CallMaskTaskDone(task.TaskId, true)
		} else {
			mapOutFiles := findFiles(".", "mr-*-"+strconv.FormatInt(int64(task.BucketId), 10))

			var kva []KeyValue

			for _, file := range mapOutFiles {
				file, err := os.Open(file)

				if err != nil {
					log.Fatalln(err.Error())
				}

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						file.Close()
						break
					}
					kva = append(kva, kv)
				}

				file.Close()
			}

			tmpFile, err := os.CreateTemp(".", "tmp-*")

			if err != nil {
				tmpFile.Close()
				return
			}

			sort.Sort(ByKey(kva))

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}

				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}

				output := reducef(kva[i].Key, values)
				fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, output)
				i = j
			}

			outFileName := "mr-out-" + strconv.FormatInt(int64(task.BucketId), 10)
			tmpFile.Close()
			os.Rename(tmpFile.Name(), outFileName)
			CallMaskTaskDone(task.TaskId, false)
		}
	}
}

func CallGetTask() GetTaskResponse {
	args := GetTaskRequest{}
	reply := GetTaskResponse{}
	ok := call("Coordinator.GetTask", &args, &reply)

	if ok && reply.TaskId != "" {
		fmt.Println("received task id ", reply.TaskId)
	} else {
		fmt.Println("call coord failed")
	}

	return reply
}

func CallMaskTaskDone(taskId string, IsMapTask bool) {
	num, err := strconv.ParseInt(taskId, 10, 64)
	if err != nil {
		fmt.Println("Error parsing string:", err)
		return
	}

	args := MarkTaskDoneRequest{
		TaskId:    num,
		IsMapTask: IsMapTask,
	}

	reply := MarkTaskDoneResponse{}

	ok := call("Coordinator.MarkDone", &args, &reply)

	if ok {
		fmt.Println("processed task ", taskId)
	} else {
		fmt.Println("err")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
