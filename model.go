package main

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)


type Model struct {
	modelId	uuid.UUID
	modelStartTime	time.Time

	taskChan <-chan *Task
	taskProcessingHandlerChan chan *Task
	taskCompleteHandlerChan chan *Task
	closeModelChan	chan bool
}

func NewModel(taskQueue *TaskQueue, taskProcessingHandlerChan chan *Task, taskCompleteHandlerChan chan *Task) *Model {

	// time.Sleep(3 * time.Second)
	time.Sleep(35 * time.Second)

	modelId := uuid.New()
	taskChan := taskQueue.Subscribe(modelId)

	model := &Model{
		modelId: modelId,
		modelStartTime: time.Now(),

		taskChan: taskChan,
		taskProcessingHandlerChan: taskProcessingHandlerChan,
		taskCompleteHandlerChan: taskCompleteHandlerChan,
		closeModelChan: make(chan bool),
	}

	go model.mainRoutine()
	fmt.Println("Model initialized, modelId: ", model.modelId)

	return model
}


func (m *Model) mainRoutine() {
	for {
		select {
		case task := <- m.taskChan:

			task.ExecModelId = m.modelId

			// Inform task manager that this model started processing
			task.Status = "processing"
			m.taskProcessingHandlerChan <- task

			output := m.predict(task.InputString)
			task.Output = output
			task.Status = "finished"

			latency := int(time.Now().Sub(task.ExecStartTime).Seconds())
			task.Latency = fmt.Sprintf("%d sec", latency)

			m.taskCompleteHandlerChan <- task

		case <- m.closeModelChan:
			return
		}
	}
}

func (m *Model) predict(inputString string) string {
	time.Sleep(10 * time.Second)

	return inputString + uuid.New().String()
}

