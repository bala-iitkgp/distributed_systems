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


type TaskDetails struct {

	StartTime time.Time
	InputFiles []string
	OutputFiles []string

}

type Coordinator struct {
	// State definitions of coordinator here.
	NMapJobs int
	NMapJobsDone int
	NReduceJobs int
	NReduceJobsDone int
	StatusLock sync.Mutex
	Tasks map[string]TaskDetails
	WaitingTaskSet map[string]bool
	ReadyTaskSet map[string]bool
	InProgressTaskSet map[string]bool
}


// RPC handlers for the worker to call.

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
	c.StatusLock.Lock()
	defer c.StatusLock.Unlock()

	for k,_ := range c.ReadyTaskSet {
		
		reply.TaskId = k
		reply.InputFiles = c.Tasks[k].InputFiles
		reply.NReduce = c.NReduceJobs

		delete(c.ReadyTaskSet, k)
		c.InProgressTaskSet[k] = true
	
		c.Tasks[k] = TaskDetails{time.Now(), c.Tasks[k].InputFiles, c.Tasks[k].OutputFiles}
		return nil
		
	}

	//Assign any yet-to-be-scheduled reduce task
	if (c.NMapJobs == c.NMapJobsDone) {
		for k,_ := range c.WaitingTaskSet {
			reply.TaskId = k
			reply.InputFiles = c.Tasks[k].InputFiles
			reply.NReduce = c.NReduceJobs

			delete(c.WaitingTaskSet, k)
			c.InProgressTaskSet[k] = true
		
			c.Tasks[k] = TaskDetails{time.Now(), c.Tasks[k].InputFiles, c.Tasks[k].OutputFiles}
			return nil

		}
	}

	//If there is no such task available, assign an already scheduled task
	//which is taking long time & Update time of that task

	currTime := time.Now()
	for k,_ := range c.InProgressTaskSet {
		
		scheduledTime := c.Tasks[k].StartTime
		if (currTime.Sub(scheduledTime).Seconds() > 10) {

			c.Tasks[k] = TaskDetails{time.Now(), c.Tasks[k].InputFiles, c.Tasks[k].OutputFiles}	
			reply.TaskId = k
			reply.InputFiles = c.Tasks[k].InputFiles
			reply.NReduce = c.NReduceJobs
			return nil
		}
	}

	reply.TaskId = "no-task"
	reply.InputFiles =[]string{""}
	reply.NReduce = c.NReduceJobs

	return nil
}

func (c *Coordinator) CompletedTask(args *CompletedTaskInput, reply *CompletedTaskOutput) error {

	taskId := args.TaskId
	outputFiles := args.OutputFiles

	c.StatusLock.Lock()
	defer c.StatusLock.Unlock()
	_,ok := c.InProgressTaskSet[taskId]
	if ok {
		if(strings.HasPrefix(taskId, "m-")) {
			c.NMapJobsDone = c.NMapJobsDone + 1
		} else if( strings.HasPrefix(taskId, "r-")) {
			c.NReduceJobsDone = c.NReduceJobsDone + 1
		}
		delete(c.InProgressTaskSet, taskId)
	}

	var reduceTask string
	if ok {
		c.Tasks[taskId] = TaskDetails{c.Tasks[taskId].StartTime, c.Tasks[taskId].InputFiles, outputFiles}
		// if this task is Map worker - update inputs of Reduce workers
		if(strings.HasPrefix(taskId, "m-")) {
			for i,v := range outputFiles {
				// this assumes the output files of each map are in order of the ReduceJobs
				reduceTask = "r-" + strconv.Itoa(i)
				c.Tasks[reduceTask] = TaskDetails{c.Tasks[reduceTask].StartTime,
												append(c.Tasks[reduceTask].InputFiles, v), c.Tasks[reduceTask].OutputFiles}
			}
		}
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

	// checks JobStatus and updates ret if job is done
	c.StatusLock.Lock()
	defer c.StatusLock.Unlock()
	if (c.NMapJobs == c.NMapJobsDone && c.NReduceJobs == c.NReduceJobsDone) {
		ret = true
	}
	

	return ret
}

func(c *Coordinator) Init(files []string, nReduce int) {

	c.NMapJobs = len(files)
	c.NReduceJobs = nReduce
	c.NMapJobsDone = 0
	c.NReduceJobsDone = 0

	c.Tasks = make(map[string]TaskDetails)
	c.ReadyTaskSet = make(map[string]bool)
	c.WaitingTaskSet = make(map[string]bool)
	c.InProgressTaskSet = make(map[string]bool)

	var taskId string
	mapPrefix := "m-"
	reducePrefix := "r-"

	var i int
	for i =0; i<c.NMapJobs; i++ {
		taskId = mapPrefix + strconv.Itoa(i)

		c.Tasks[taskId] = TaskDetails{time.Now(), []string{files[i]}, []string{} }

		c.ReadyTaskSet[taskId] = true
	}

	for i =0; i<nReduce; i++ {
		taskId = reducePrefix + strconv.Itoa(i)

		c.Tasks[taskId] = TaskDetails{time.Now(), []string{}, []string{}}

		c.WaitingTaskSet[taskId] = true
	}

}
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.Init(files, nReduce)
	c.server()

	return &c
}