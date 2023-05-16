package main

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

var cache = sync.Map{}

type PushBody struct {
	InputString string `json:"input"`
}

type PushJson struct {
	Id string `json:"id"`
}

type StatusJson struct {
	Status string `json:"status"`
	StatusCode string `json:"statusCode"`
}

type DataJson struct {
	Input string `json:"status"`
	Output string `json:"output"`
	Latency string `json:"latency"`
}

func main() {

	app := fiber.New()
	numSecRollingAvg := 3
	numSecSpinDownDelay := 10
	taskManager, err := NewTaskManager(numSecRollingAvg, numSecSpinDownDelay)
	if err != nil {
		fmt.Println("error while initializing taskManager", err)
	}


	app.Post("/push", func(ctx *fiber.Ctx) error {
		fmt.Println("here0")
		input := ctx.Query("input")
		// pushBody := &PushBody{}
		// err := ctx.BodyParser(pushBody)
		fmt.Println(input)

		// if !exists {
		// 	fmt.Println("error while parsing push", err)
		// 	return ctx.SendStatus(404)
		// }

		taskManager.pushHandlerChan <- input
		taskId := <-taskManager.pushReturnChan
		return ctx.JSON(&PushJson{Id: taskId.String()})
	})


	app.Get("/status/:taskIdStr", func(ctx *fiber.Ctx) error {
		/*
		Query status from TaskManager
		Returns 404 if task is not found
		*/
		taskIdStr := ctx.Params("taskIdStr")

		taskID, _ := uuid.Parse(taskIdStr)
		fmt.Println(taskIdStr, taskID.String())

		taskManager.statusHandlerChan <- taskID
		status := <-taskManager.statusReturnChan

		if len(status) > 0{
			return ctx.JSON(&StatusJson{Status: status, StatusCode: strconv.Itoa(200)})
		} else {
			return ctx.JSON(&StatusJson{Status: status,  StatusCode: strconv.Itoa(404)})
		}
	})

	app.Get("/data/:taskIdStr", func(ctx *fiber.Ctx) error {
		taskIdStr := ctx.Params("taskIdStr")
		taskID, _ := uuid.Parse(taskIdStr)
		fmt.Println(taskIdStr, taskID.String())
		/*
		Query output data from TaskManager
		Returns 404 if task is not found
		Returns null values for output and latency if task is not complete
		*/
		// HEREREE!!
		taskManager.dataHandlerChan <- taskID
		dataJson := <-taskManager.dataReturnChan

		return ctx.JSON(*dataJson)
	})

	app.Listen(":8002")
}
