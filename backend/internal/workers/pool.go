package workers

import (
	"context"
	"sync"

	"github.com/pcristin/monad-faucet/pkg/logger"
)

// WorkerPool represents a pool of workers that can process tasks
type WorkerPool struct {
	name        string
	numWorkers  int
	taskQueue   chan Task
	workerWg    sync.WaitGroup
	ctx         context.Context
	cancelFunc  context.CancelFunc
	initialized bool
}

// Task represents a unit of work to be processed by a worker
type Task interface {
	Process() error
	ID() string
	Type() string
}

// NewWorkerPool creates a new worker pool with the specified number of workers
func NewWorkerPool(name string, numWorkers int, queueSize int) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		name:        name,
		numWorkers:  numWorkers,
		taskQueue:   make(chan Task, queueSize),
		ctx:         ctx,
		cancelFunc:  cancel,
		initialized: false,
	}
}

// Start initializes and starts the worker pool
func (p *WorkerPool) Start() {
	if p.initialized {
		logger.Warn("Worker pool %s already started", p.name)
		return
	}

	logger.Info("Starting worker pool %s with %d workers", p.name, p.numWorkers)

	// Start the workers
	p.workerWg.Add(p.numWorkers)
	for i := 0; i < p.numWorkers; i++ {
		workerID := i + 1
		go p.startWorker(workerID)
	}

	p.initialized = true
}

// Stop gracefully shuts down the worker pool
func (p *WorkerPool) Stop() {
	if !p.initialized {
		logger.Warn("Worker pool %s not started", p.name)
		return
	}

	logger.Info("Stopping worker pool %s", p.name)

	// Signal all workers to stop
	p.cancelFunc()

	// Close the task queue to prevent new tasks
	close(p.taskQueue)

	// Wait for all workers to finish
	p.workerWg.Wait()

	p.initialized = false
	logger.Info("Worker pool %s stopped", p.name)
}

// Submit adds a task to the worker pool queue
func (p *WorkerPool) Submit(task Task) bool {
	if !p.initialized {
		logger.Error("Cannot submit task to uninitialized worker pool %s", p.name)
		return false
	}

	// Try to submit the task to the queue
	select {
	case p.taskQueue <- task:
		logger.Debug("Task %s of type %s submitted to worker pool %s", task.ID(), task.Type(), p.name)
		return true
	case <-p.ctx.Done():
		logger.Warn("Worker pool %s is shutting down, task %s rejected", p.name, task.ID())
		return false
	default:
		logger.Warn("Worker pool %s queue is full, task %s rejected", p.name, task.ID())
		return false
	}
}

// startWorker runs a worker goroutine that processes tasks from the queue
func (p *WorkerPool) startWorker(id int) {
	defer p.workerWg.Done()

	logger.Debug("Worker %d started in pool %s", id, p.name)

	for {
		select {
		case task, ok := <-p.taskQueue:
			if !ok {
				// Channel closed, exit worker
				logger.Debug("Worker %d in pool %s exiting (queue closed)", id, p.name)
				return
			}

			// Process the task
			logger.Debug("Worker %d in pool %s processing task %s of type %s",
				id, p.name, task.ID(), task.Type())

			// Check if the task has a custom processor
			baseTask, hasCustomProcessor := task.(interface {
				HasCustomProcessor() bool
				RunCustomProcessor(interface{}) error
			})

			var err error
			if hasCustomProcessor && baseTask.HasCustomProcessor() {
				// Use the custom processor
				err = baseTask.RunCustomProcessor(task)
			} else {
				// Use the default Process method
				err = task.Process()
			}

			if err != nil {
				logger.Error("Worker %d in pool %s failed to process task %s: %v",
					id, p.name, task.ID(), err)
			} else {
				logger.Debug("Worker %d in pool %s completed task %s", id, p.name, task.ID())
			}

		case <-p.ctx.Done():
			// Context cancelled, exit worker
			logger.Debug("Worker %d in pool %s exiting (context cancelled)", id, p.name)
			return
		}
	}
}

// QueueSize returns the current number of tasks in the queue
func (p *WorkerPool) QueueSize() int {
	return len(p.taskQueue)
}

// IsRunning returns whether the worker pool is running
func (p *WorkerPool) IsRunning() bool {
	return p.initialized
}

// Name returns the name of the worker pool
func (p *WorkerPool) Name() string {
	return p.name
}
