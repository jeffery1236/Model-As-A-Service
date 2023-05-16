package main

import (
	"errors"
	"sync"

	"github.com/google/uuid"
)

type TaskQueue struct {
	mu     *sync.RWMutex
	subs   map[uuid.UUID]chan *Task
	closed bool
}

func (tq *TaskQueue) RegisterSub(modelId uuid.UUID) (error) {
	tq.mu.RLock()
	defer tq.mu.RUnlock()

	if tq.closed {
		return nil
	}

	_, exists := tq.subs[modelId]
	if exists {
		return errors.New("Model ID alr exists")
	}

	tq.subs[modelId] = make(chan *Task)
	return nil
}

func (tq *TaskQueue) Publish(modelId uuid.UUID, msg *Task) (error) {
	tq.mu.RLock()
	defer tq.mu.RUnlock()

	if tq.closed {
	  return nil
	}

	ch, exists := tq.subs[modelId]
	if !exists {
		return errors.New("Model ID does not exist")
	}

	go func(ch chan *Task) {
		ch <- msg
	}(ch)

	return nil
  }

func (tq *TaskQueue) Subscribe(modelId uuid.UUID) chan *Task {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	ch := make(chan *Task)
	tq.subs[modelId] = ch
	return ch
}

func (tq *TaskQueue) Close() {
	tq.mu.Lock()
	defer tq.mu.Unlock()

	if !tq.closed {
		tq.closed = true
		for _, ch := range tq.subs {
		  close(ch)
		}
	}
}


