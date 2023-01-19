package mr

import "C"
import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mu               sync.Mutex
	muRes            sync.Mutex
	file             []string
	nReduce          int
	mapTask          []MapTask
	mapTaskCount     int32
	mapFinishedCount SafeNumber
	task             chan MapTask
	checkEvent       chan MapTask
	done             chan struct{}
}

type SafeNumber struct {
	val int32
	m   sync.Mutex
}

func (i *SafeNumber) Get() int32 {
	i.m.Lock()
	defer i.m.Unlock()
	return i.val
}

func (i *SafeNumber) Set() {
	i.m.Lock()
	defer i.m.Unlock()
	i.val++
}

type MapTask struct {
	Id        int
	File      string
	Done      bool
	Finished  bool
	startTime int64
}

func LogPrintf(format string, a ...interface{}) {
	str := fmt.Sprintf(format, a...)
	fmt.Printf("[%v][pid=%v]: 	%v \n", time.Now().Format("01-02-2006 15:04:05.0000"), os.Getpid(), str)
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) MapRequest(args *ExampleArgs, reply *MapQuestResponse) error {
	c.mu.Lock()
	LogPrintf("-------process maprequest")
	defer c.mu.Unlock()
	if c.mapFinishedCount.Get() >= int32(len(c.mapTask)) {
		//reply.MapTask.finished = true
		fmt.Println("***********map done")
		reply.MapTask = MapTask{
			Id:        666,
			File:      "",
			Done:      false,
			Finished:  false,
			startTime: 0,
		}
		return nil
	}
	//for _, task := range c.MapTask {
	//	<-c.task
	//	if !task.done {
	//		reply.MapTaskId = task.id
	//		reply.file = task.file
	//		task.done = true
	//	}
	//}
	//startTime := time.Now().Unix()
out:
	for {
		select {
		case task := <-c.task:
			reply.MapTask = task
			LogPrintf("assign task = %v , task id =  %v", task.File, task.Id)
			//task.startTime = time.Now().Unix()
			c.mapTask[task.Id].startTime = time.Now().Unix()
			//c.checkEvent <- task
			//fmt.Printf("task assigned in check %v", task.File)
			break out
		case <-c.done:
			fmt.Println("receive done")
			return nil
		}

	}

	return nil
}

func (c *Coordinator) MapFinish(args *MapFinishRequest, reply *MapFinishReply) error {
	c.muRes.Lock()
	defer c.muRes.Unlock()
	LogPrintf("------process map finish")
	currentTime := time.Now().Unix()
	if currentTime-c.mapTask[args.MapTaskId].startTime > 10 &&
		c.mapTask[args.MapTaskId].Finished == false {
		LogPrintf("maptask time out which taskId is %v", args.MapTaskId)
		c.task <- c.mapTask[args.MapTaskId]
		return nil
	}

	c.mapFinishedCount.Set()
	// c.mapFinishedCount++
	LogPrintf("map finish task id is %v", args.MapTaskId)
	LogPrintf("map finish count is %v/%v", c.mapFinishedCount.Get(), c.mapTaskCount)
	c.mapTask[args.MapTaskId].Finished = true
	if c.mapFinishedCount.Get() == c.mapTaskCount {
		c.done <- struct{}{}
		LogPrintf("done channel")
	}
	LogPrintf("map finish over")
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.file = files
	c.nReduce = nReduce
	fmt.Println(len(files))
	c.done = make(chan struct{}, 1)
	c.mapTask = make([]MapTask, len(files))
	c.mapTaskCount = int32(len(files))
	c.task = make(chan MapTask, len(files))
	c.checkEvent = make(chan MapTask, len(files))
	//go c.checkEventLoop(c.checkEvent)
	for i := 0; i < len(files); i++ {
		maptask := MapTask{
			Id:   i,
			File: files[i],
			Done: false,
		}
		c.mapTask[i] = maptask
		fmt.Printf("init map task is %v \n", maptask)
		c.task <- maptask
	}

	c.server()
	return &c
}

//func (c *Coordinator) checkEventLoop(check chan MapTask) {
//	for {
//		select {
//		case mapsTask := <-check:
//			currentTime := time.Now().Unix()
//			if currentTime-mapsTask.startTime >= 10 && !findMapTaskFinished() {
//				c.task <- mapsTask
//			}
//		}
//	}
//}
