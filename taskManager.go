package main

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)


type Task struct {
	TaskId uuid.UUID
	Status string
	InputString string
	Output string
	ExecStartTime time.Time
	ExecModelId uuid.UUID
	Latency string
}

type TaskManager struct {

	epochMillis	int
	numSecRollingAvg	int
	numSecSpinDownDelay	int

	modelMap	map[string]*Model

	modelInitChan chan *Model

	pushHandlerChan chan string
	statusHandlerChan chan uuid.UUID
	dataHandlerChan chan uuid.UUID
	taskCompleteHandlerChan chan *Task
	taskProcessingHandlerChan chan *Task

	pushReturnChan chan uuid.UUID
	statusReturnChan chan string
	dataReturnChan chan *DataJson

	closeTaskManagerChan chan bool
}

func NewTaskManager(numSecRollingAvg int, numSecSpinDownDelay int) (*TaskManager, error) {

	if (numSecRollingAvg <= 1) {
		return nil, errors.New("Number of seconds to consider rolling average must be more than 1")
	}

	taskManager := &TaskManager{
		numSecRollingAvg: numSecRollingAvg,
		numSecSpinDownDelay: numSecSpinDownDelay,

		modelInitChan: make(chan *Model),

		pushHandlerChan: make(chan string),
		statusHandlerChan: make(chan uuid.UUID),
		dataHandlerChan: make(chan uuid.UUID),
		taskCompleteHandlerChan: make(chan *Task),
		taskProcessingHandlerChan: make(chan *Task),

		pushReturnChan: make(chan uuid.UUID),
		statusReturnChan: make(chan string),
		dataReturnChan: make(chan *DataJson),

		closeTaskManagerChan: make(chan bool),
	}

	go taskManager.mainRoutine()

	return taskManager, nil
}

func (t *TaskManager) findModeltoPublish(modelsMap map[uuid.UUID]*Model, runningModelTaskMap map[uuid.UUID]*Task) uuid.UUID {
	if len(runningModelTaskMap) == len(modelsMap) {
		// All models are busy -> publish to task that started the earliest
		earliestStartTime := time.Now()
		earliestModelId := uuid.Nil
		for modelId, task := range runningModelTaskMap {
			if earliestStartTime.Sub(task.ExecStartTime) > 0 {
				earliestStartTime = task.ExecStartTime // NOTE: Assumes that if task was submitted/assigned earlier, it would be executed earlier -> might be wrong
				earliestModelId = modelId
			}
		}
		return earliestModelId
	} else {
		// Find first avail Model
		for modelId, _ := range modelsMap {
			_, exists := runningModelTaskMap[modelId]
			if !exists { // model is not currently running
				return modelId
			}
		}

		return uuid.Nil // should never reach this case
	}
}

func (t *TaskManager) mainRoutine() {

	ticker := time.NewTicker(time.Duration(t.numSecRollingAvg) * time.Second)
	numNewTasksLastSec := 0
	avgTasksPerSec := 0
	timeAccumulatedForSpinDown := 0

	taskQueue := &TaskQueue{
		mu:	new(sync.RWMutex),
		subs: make(map[uuid.UUID]chan *Task),
		closed: false,
	}
	// taskQueue := make([]Task, 0) // unbounded channel
	taskMap :=	make(map[uuid.UUID]*Task)
	unassignedTasks := make([]*Task, 0)
	modelsMap := make(map[uuid.UUID]*Model)
	runningModelTaskMap := make(map[uuid.UUID]*Task)

	// Have at least 1 model up
	model := NewModel(taskQueue, t.taskProcessingHandlerChan, t.taskCompleteHandlerChan)
	modelsMap[model.modelId] = model

	for {
		select {
		case inputString := <- t.pushHandlerChan:
			// Add task to taskQueue and increment active tasks
			// fmt.Println("push", inputString)
			newTask := &Task{
				TaskId: uuid.New(),
				Status: "",
				InputString: inputString,
				ExecStartTime: time.Now(),
			}

			modelIdToPublish := t.findModeltoPublish(modelsMap, runningModelTaskMap)

			if modelIdToPublish != uuid.Nil {
				taskQueue.Publish(modelIdToPublish, newTask)
				// fmt.Println("Task assigned")
				newTask.Status = "queued"
			} else {
				unassignedTasks = append(unassignedTasks, newTask)
			}

			taskMap[newTask.TaskId] = newTask
			numNewTasksLastSec += 1

			t.pushReturnChan <- newTask.TaskId

		case taskId := <- t.statusHandlerChan:
			// fmt.Println("TM Status")
			task, taskExists := taskMap[taskId]
			if (!taskExists) {
				// return nil
				t.statusReturnChan <- ""
			} else {
				fmt.Println(task.Status)
				t.statusReturnChan <- task.Status
			}

		case taskId := <- t.dataHandlerChan:
			task, taskExists := taskMap[taskId]
			if (!taskExists) {
				t.dataReturnChan <- nil
				} else {
				var output string
				var latency string
				if task.Status == "finished" {
					output = task.Output
					latency = task.Latency
					// fmt.Println("here5", output, latency)
				} else {
					// fmt.Println("here6")
					output = ""
					latency = ""
				}

				dataJson := &DataJson{
					Input: task.InputString,
					Output: output,
					Latency: latency,
				}

				fmt.Println(task.InputString, dataJson.Input, dataJson.Output, dataJson.Latency)

				t.dataReturnChan <- dataJson
			}

		case task := <- t.taskProcessingHandlerChan:
			/*
			Add to runningModelsMap
			Update taskMap
			*/
			// fmt.Println("TM Proc")
			runningModelTaskMap[task.ExecModelId] = task
			taskMap[task.TaskId] = task

		case task := <- t.taskCompleteHandlerChan:
			/*
			Remove from runningModelsMap
			Update taskMap
			*/
			// fmt.Println("TM Complete")
			delete(runningModelTaskMap, task.ExecModelId)
			taskMap[task.TaskId] = task

		case model := <- t.modelInitChan:
			modelsMap[model.modelId] = model

		case <- t.closeTaskManagerChan:
			for _, model := range modelsMap {
				model.closeModelChan <- true
			}
			return

		case <-ticker.C:

			avgTasksPerSec = ( (t.numSecRollingAvg - 1) * avgTasksPerSec + numNewTasksLastSec ) / t.numSecRollingAvg // Update avgTasksPerSec
			// fmt.Println("Tick, avgTasksPerSec", avgTasksPerSec, "num models:", len(modelsMap))
			if avgTasksPerSec > len(modelsMap) { // Compare avgTasksPerSec with numModelsUp
				// Time to spin up to meet demand
				timeAccumulatedForSpinDown = 0

				go func(modelInitChan chan *Model) { // init model
					model := NewModel(taskQueue, t.taskProcessingHandlerChan, t.taskCompleteHandlerChan)
					modelInitChan <- model
				}(t.modelInitChan)

				// fmt.Println("Spin up", len(modelsMap), avgTasksPerSec)

			} else if (avgTasksPerSec < len(modelsMap) && len(modelsMap) > 1) {
				// Increment timeAccumulatedForSpinDown -> Spin down once timeAccumulatedForSpinDown >=
				timeAccumulatedForSpinDown += 1
				// fmt.Println("Spin down",len(modelsMap), avgTasksPerSec, timeAccumulatedForSpinDown)

				if timeAccumulatedForSpinDown >= t.numSecSpinDownDelay {

					for modelId, model := range modelsMap {
						_, exists := runningModelTaskMap[modelId]
						if !exists {
							go func(model *Model) { // ensure close is not blocking
								model.closeModelChan <- true // remove model from modelsMap
								// fmt.Println("Spin down success",len(modelsMap))
							}(model)

							timeAccumulatedForSpinDown = 0

							break
						}

					}

				}

			} else {
				timeAccumulatedForSpinDown = 0
			}

			// Assign unassignedTasks if models are available
			for _, task := range unassignedTasks {
				modelIdToPublish := t.findModeltoPublish(modelsMap, runningModelTaskMap)

				if modelIdToPublish != uuid.Nil {
					taskQueue.Publish(modelIdToPublish, task)
					// fmt.Println("Task assigned")
					task.Status = "queued"

					taskMap[task.TaskId] = task
					unassignedTasks = unassignedTasks[1:]

				} else {
					break
				}
			}
		}
	}

}


