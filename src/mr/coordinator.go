package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "strconv"
//import "fmt"
import "strings"

type Coordinator struct {
	// Your definitions here.

}


type TaskDetails struct {

	StartTime time.Time
	InputFiles []string
	OutputFiles []string

}

// variables

var NMapJobs, NMapJobsDone, NReduceJobs, NReduceJobsDone int

var TaskLock, StatusLock sync.Mutex

var Tasks map[string]TaskDetails

var WaitingTaskSet, ReadyTaskSet, InProgressTaskSet map[string]bool

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


func (c *Coordinator) AssignTask(args *AssignTaskInput, reply *AssignTaskOutput) error {


	//Assign any yet-to-be-scheduled map task
	//fmt.Println("Got a request asking for a task")
	StatusLock.Lock()
	TaskLock.Lock()
	var isLocked bool = true
	for k,_ := range ReadyTaskSet {
		
		reply.TaskId = k
		reply.InputFiles = Tasks[k].InputFiles
		reply.NReduce = NReduceJobs

		delete(ReadyTaskSet, k)
		InProgressTaskSet[k] = true
	
		Tasks[k] = TaskDetails{time.Now(), Tasks[k].InputFiles, Tasks[k].OutputFiles}
		StatusLock.Unlock()
		TaskLock.Unlock()
		isLocked = false
		return nil
		
	}
	if (isLocked) {
		StatusLock.Unlock()
		TaskLock.Unlock()
	}
	//Assign any yet-to-be-scheduled reduce task

	StatusLock.Lock()
	isLocked = true
	if (NMapJobs == NMapJobsDone) {
		for k,_ := range WaitingTaskSet {
			
			TaskLock.Lock()
			reply.TaskId = k
			reply.InputFiles = Tasks[k].InputFiles
			reply.NReduce = NReduceJobs

			delete(WaitingTaskSet, k)
			InProgressTaskSet[k] = true
		
			Tasks[k] = TaskDetails{time.Now(), Tasks[k].InputFiles, Tasks[k].OutputFiles}
			TaskLock.Unlock()
			StatusLock.Unlock()
			isLocked = false
			return nil

		}
	}
	if (isLocked) {
		StatusLock.Unlock()
	}
	

	//If there is no such task available, assign an already scheduled task
	//which is taking long time & Update time of that task

	currTime := time.Now()
	StatusLock.Lock()
	TaskLock.Lock()
	isLocked = true
	for k,_ := range InProgressTaskSet {
		
		scheduledTime := Tasks[k].StartTime
		if (currTime.Sub(scheduledTime).Seconds() > 10) {

			Tasks[k] = TaskDetails{time.Now(), Tasks[k].InputFiles, Tasks[k].OutputFiles}
			
			reply.TaskId = k
			reply.InputFiles = Tasks[k].InputFiles
			reply.NReduce = NReduceJobs

			TaskLock.Unlock()
			StatusLock.Unlock()
			isLocked = false
			return nil
		}
	}
	if (isLocked) {
		StatusLock.Unlock()
		TaskLock.Unlock()
	}
	
	reply.TaskId = "no-task"
	reply.InputFiles =[]string{""}
	reply.NReduce = NReduceJobs

	return nil
}

func (c *Coordinator) CompletedTask(args *CompletedTaskInput, reply *CompletedTaskOutput) error {

	//fmt.Println("Some worker completed their task")
	taskId := args.TaskId
	outputFiles := args.OutputFiles

	

	StatusLock.Lock()
	_,ok := InProgressTaskSet[taskId]
	if ok {
		if(strings.HasPrefix(taskId, "m-")) {
			NMapJobsDone = NMapJobsDone + 1
		} else if( strings.HasPrefix(taskId, "r-")) {
			NReduceJobsDone = NReduceJobsDone + 1
		}
		//fmt.Println("Status of Jobs Done") 
		//fmt.Println("... MapJobs : ", NMapJobsDone, " out of ", NMapJobs)
		//fmt.Println("... ReduceJobs : ", NReduceJobsDone, " out of ", NReduceJobs)
		delete(InProgressTaskSet, taskId)
	}
	StatusLock.Unlock()

	var reduceTask string
	if ok {
		TaskLock.Lock()
		Tasks[taskId] = TaskDetails{Tasks[taskId].StartTime, Tasks[taskId].InputFiles, outputFiles}

		// if this task is Map worker - update inputs of Reduce workers
		if(strings.HasPrefix(taskId, "m-")) {
			for i,v := range outputFiles {
				// this assumes the output files of each map are in order of the ReduceJobs
				reduceTask = "r-" + strconv.Itoa(i)
				Tasks[reduceTask] = TaskDetails{Tasks[reduceTask].StartTime,
												append(Tasks[reduceTask].InputFiles, v), Tasks[reduceTask].OutputFiles}
			}
		}

		TaskLock.Unlock()
		reply.Ack = "Thanks"
	} else {
		reply.Ack = "Already Done. Thanks anyway"
	}

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
	// checks JobStatus and updates ret if job is done
	StatusLock.Lock()
	if (NMapJobs == NMapJobsDone && NReduceJobs == NReduceJobsDone) {
		ret = true
	}
	StatusLock.Unlock()

	return ret
}

func Init(files []string, nReduce int) {

	NMapJobs = len(files)
	NReduceJobs = nReduce
	NMapJobsDone = 0
	NReduceJobsDone = 0

	Tasks = make(map[string]TaskDetails)
	ReadyTaskSet = make(map[string]bool)
	WaitingTaskSet = make(map[string]bool)
	InProgressTaskSet = make(map[string]bool)

	var taskId string
	mapPrefix := "m-"
	reducePrefix := "r-"

	var i int
	for i =0; i<NMapJobs; i++ {
		taskId = mapPrefix + strconv.Itoa(i)

		Tasks[taskId] = TaskDetails{time.Now(), []string{files[i]}, []string{} }

		ReadyTaskSet[taskId] = true
	}

	for i =0; i<nReduce; i++ {
		taskId = reducePrefix + strconv.Itoa(i)

		Tasks[taskId] = TaskDetails{time.Now(), []string{}, []string{}}

		WaitingTaskSet[taskId] = true
	}

}
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	Init(files, nReduce)

	c.server()
	return &c
}