package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "strings"
import "strconv"
import "sort"
import "os"
import "time"
import "encoding/json"
import "io/ioutil"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}


// for sorting by key.
type ValueByKey []KeyValue

// for sorting by key.
func (a ValueByKey) Len() int           { return len(a) }
func (a ValueByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ValueByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	//Periodically ask for a task
	
	var ok bool = true
	for ; ok ; {

		// ok = AskTask()

		args := AssignTaskInput{}

		args.Request = "Give me a task"

		reply := AssignTaskOutput{}

		ok = call("Coordinator.AssignTask", &args, &reply)

		if (ok && reply.TaskId!="no-task") {

			//fmt.Println("A task is assigned: %v", reply.TaskId)
			
			//If a task is assigned, complete the task and notify the co-ordinator
			taskOutput := ExecuteTask(mapf, reducef, reply)

			ok = ReportTask(taskOutput)

		}


		time.Sleep(time.Second)

	}

}

func ExecuteMap(mapf func(string, string) []KeyValue, 
	taskId string, inputFiles []string, nReduce int) CompletedTaskInput {

	intermediate := []KeyValue{}
	for _, filename := range inputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//fmt.Println("Length of the intermediate key value pairs generated ", len(ValueByKey(intermediate)))


	intermediateFilePrefix := "mr"

	output := CompletedTaskInput{}
	output.TaskId = taskId
	output.OutputFiles = []string{}

	var i, j int
	//fmt.Println("NReduceJobs is equal to ", nReduce)
	for  i = 0; i < nReduce; i++ {
		//fmt.Println("Trying to generate intermediate output for ", i, "th ReduceJob")
		ofileName := intermediateFilePrefix + taskId[1:] + "-" + strconv.Itoa(i)
		ofile, err := os.Create(ofileName)
		//fmt.Println("Opened an intermediate file: ", ofileName)
		if err != nil {
			log.Fatalf("cannot open %v", ofileName)
		}
		enc := json.NewEncoder(ofile)

		for  j = 0; j < len(intermediate); j++ {
			if( ihash(intermediate[j].Key) % nReduce == i){
				// fmt.Println("Writing Key ", intermediate[j].Key, " Value ", intermediate[j].Value)
				err := enc.Encode(&intermediate[j])
				if err != nil {
					log.Fatalf("Cannot Encode for file %v", ofileName)
				}
			}
		}

		output.OutputFiles = append(output.OutputFiles, ofileName)	
	}

	//fmt.Println("Completed Execution of task: %v", taskId)

	return output

}

func ExecuteReduce(reducef func(string, []string) string, 
	taskId string, inputFiles []string) CompletedTaskInput {
	
	intermediate := []KeyValue{}
	for _, fileName := range inputFiles {
		//fmt.Println("In Executing Reduce, trying to open ", fileName)
		file, _ := os.Open(fileName)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv) ; err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ValueByKey(intermediate))

	//fmt.Println("Len of Keys for this Reduce Job: ", len(ValueByKey(intermediate)))

	oname := "mr-out" + taskId[1:]
	ofile, _ := os.Create(oname)

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
		//fmt.Println("Output of Reduce Job. Key: ", intermediate[i].Key, " Value: ", output)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	//fmt.Println("Completed Execution of the task: %v", taskId)

	return CompletedTaskInput{taskId, []string{oname}}
}

func ExecuteTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, 
	task AssignTaskOutput) CompletedTaskInput {

	taskId := task.TaskId
	inputFiles := task.InputFiles
	nReduce := task.NReduce

	//fmt.Println("Started Executing task: %v",taskId)

	if (strings.HasPrefix(taskId, "m-" )) {
		return ExecuteMap(mapf, taskId, inputFiles, nReduce)
	} else if (strings.HasPrefix(taskId, "r-")) {
		return ExecuteReduce(reducef, taskId, inputFiles)
	}

	//fmt.Println("Error: This is not a valid task to Execute")
	return CompletedTaskInput{}
}

func ReportTask(args CompletedTaskInput) bool {

	reply := CompletedTaskOutput{}

	ok := call("Coordinator.CompletedTask", &args, &reply)

	//fmt.Println("Completed the Report of the task: %v", args.TaskId)

	return ok
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