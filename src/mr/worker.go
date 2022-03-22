package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"
)

var SleepTime time.Duration = 1 * time.Second
var NReduce int = 10

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

	// uncomment to send the Example RPC to the master.
	// CallExample()

	for {
		args := WorkerMessage{}
		reply := MasterMessage{}
		CallAssignTask(&args, &reply)
		if reply.MasterMessageType == 3 {
			return
		} else if reply.MasterMessageType == 2 {
			time.Sleep(SleepTime)
		} else if reply.MasterMessageType == 1 {
			if reply.TaskType == 1 {
				tmpFileArr := DoMap(mapf, reply.TheMapTask)
				if len(tmpFileArr) != 0 {
					nargs := WorkerMessage{}
					nreply := MasterMessage{}
					nargs.TheMapTask = reply.TheMapTask
					nargs.TaskType = reply.TaskType
					nargs.SessionId = reply.SessionId
					CallConfirmTask(&nargs, &nreply)
					fmt.Printf("MaterMessageType is %v  MapTaskId is %v\n", nreply.MasterMessageType, nargs.TheMapTask.MapTaskId)
					if nreply.MasterMessageType == 4 {
						DoMapAfter(false, tmpFileArr, nargs.TheMapTask)
					} else {
						DoMapAfter(true, tmpFileArr, nargs.TheMapTask)
					}
				}
			} else if reply.TaskType == 2 {
				tmpFile := DoReduce(reducef, reply.TheReduceTask)
				if tmpFile != nil {
					nargs := WorkerMessage{}
					nreply := MasterMessage{}
					nargs.TaskType = 2
					nargs.TheReduceTask = reply.TheReduceTask
					nargs.SessionId = reply.SessionId
					CallConfirmTask(&nargs, &nreply)
					fmt.Printf("MaterMessageType is %v  ReduceTaskId is %v\n", nreply.MasterMessageType, nargs.TheReduceTask.ReduceTaskId)
					if nreply.MasterMessageType == 4 {
						DoReduceAfter(false, tmpFile, nargs.TheReduceTask)
					} else {
						DoReduceAfter(true, tmpFile, nargs.TheReduceTask)
					}
				}
			}
		}
	}
}

func DoReduce(reducef func(string, []string) string, theReduceTask ReduceTask) *os.File {
	pwd, _ := os.Getwd()
	// tmpDir := pwd + "/tmp"
	tmpDir := pwd
	fileInfoList, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		log.Fatal(err)
	}
	fileNames := []string{}
	r, _ := regexp.Compile("mr-([0-9]+)-" + strconv.Itoa(theReduceTask.ReduceTaskId))
	for i := range fileInfoList {
		if r.MatchString(fileInfoList[i].Name()) {
			fileNames = append(fileNames, fileInfoList[i].Name())
		}
	}
	files := make([]*os.File, len(fileNames))
	kva := make([]KeyValue, 0)
	for i := range files {
		// files[i], err = os.Open("./tmp/" + fileNames[i])
		files[i], err = os.Open(fileNames[i])
		if err != nil {
			log.Fatal(err)
		}
		defer files[i].Close()
		dec := json.NewDecoder(files[i])
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))

	tmpFile, err := ioutil.TempFile("", "tmp-res-"+strconv.Itoa(theReduceTask.ReduceTaskId))
	if err != nil {
		log.Fatal(err)
	}
	// enc := json.NewEncoder(tmpFile)

	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[i].Key == kva[j].Key {
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

	// tmpFile, err := ioutil.TempFile("./tmp/res", "tmp-res-"+strconv.Itoa(theReduceTask.ReduceTaskId))
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// enc := json.NewEncoder(tmpFile)
	// for _, kv := range kva {
	// 	err = enc.Encode(&kv)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// }
	// os.Rename(tmpFile.Name(), "mr-out-"+strconv.Itoa(theReduceTask.ReduceTaskId))
	return tmpFile
}

func DoReduceAfter(state bool, tmpFile *os.File, theReduceTask ReduceTask) {
	if state {
		os.Rename(tmpFile.Name(), "mr-out-"+strconv.Itoa(theReduceTask.ReduceTaskId))
	} else {
		os.Remove(tmpFile.Name())
	}
}

func DoMap(mapf func(string, string) []KeyValue, theMapTask MapTask) []*os.File {
	targetPath := theMapTask.TargetPath
	file, err := os.Open(targetPath)
	fmt.Printf("targetPath is %v\n", targetPath)
	if err != nil {
		log.Fatalf("cannot open %v", targetPath)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", targetPath)
	}
	kva := mapf(targetPath, string(content))
	// tmpFile, err := ioutil.TempFile("./tmp", "tmpfiles-"+strconv.Itoa(theMapTask.MapTaskId))
	if len(kva) == 0 {
		fmt.Print("Here is kva\n")
	}
	fmt.Println()
	for i, v := range kva {
		fmt.Printf("%v %v %v\n", i, v.Key, v.Value)
	}
	fmt.Println()
	// if err != nil {
	// 	log.Fatalf("cannot create temp file %v", err)
	// }
	// defer tmpFile.Close()
	// enc := json.NewEncoder(tmpFile)
	// for _, v := range kva {
	// 	err = enc.Encode(&v)
	// 	if err != nil {
	// 		log.Fatalln(err)
	// 	}
	// }
	// return true

	tmpFileArr := make([]*os.File, NReduce)
	encArr := make([]*json.Encoder, NReduce)
	for i := range tmpFileArr {
		tmpFileArr[i], err = ioutil.TempFile("", "tmpfiles-"+strconv.Itoa(theMapTask.MapTaskId)+"-"+strconv.Itoa(i))
		if err != nil {
			dir, _ := os.Getwd()
			fmt.Printf("here is my dir %v", dir)
			log.Fatalf("cannot create temp file %v", err)
		}
		defer tmpFileArr[i].Close()
		encArr[i] = json.NewEncoder(tmpFileArr[i])
	}
	for _, v := range kva {
		i := ihash(v.Key) % NReduce
		err = encArr[i].Encode(&v)
		if err != nil {
			log.Fatalln(err)
		}
	}
	// for i := range tmpFileArr {
	// 	os.Rename(tmpFileArr[i].Name(), "mr-"+strconv.Itoa(theMapTask.MapTaskId)+"-"+strconv.Itoa(i))
	// }
	return tmpFileArr
}

func DoMapAfter(state bool, tmpFileArr []*os.File, theMapTask MapTask) {
	if state {
		for i := range tmpFileArr {
			err := os.Rename(tmpFileArr[i].Name(), "mr-"+strconv.Itoa(theMapTask.MapTaskId)+"-"+strconv.Itoa(i))
			if err != nil {
				log.Fatal(err)
			}
		}
	} else {
		for i := range tmpFileArr {
			err := os.Remove(tmpFileArr[i].Name())
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

func CallAssignTask(args *WorkerMessage, reply *MasterMessage) {
	call("Master.AssignTask", &args, &reply)
}

func CallConfirmTask(args *WorkerMessage, reply *MasterMessage) {
	call("Master.ConfirmTask", &args, &reply)
}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
