package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Ask for more tasks
type GetTaskRequest struct{}

type GetTaskResponse struct {
	TaskId        string
	IsMapWorker   bool
	InputFileName string
	NReduce       int
	BucketId      int
}

// Tell the coord that task was done
type MarkTaskDoneRequest struct {
	TaskId    int64
	IsMapTask bool
}

type MarkTaskDoneResponse struct{}
