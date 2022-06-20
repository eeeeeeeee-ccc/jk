package product

import (
	"container/list"
	"go.uber.org/atomic"
	"sync"
	"time"
)

type IoWorkerPool struct {
	queue    *list.List
	ioWorker *IoWorker
	lock     sync.RWMutex
	threadPoolShutDownFlag *atomic.Bool
}

func initIoWorkerPool(ioWorker *IoWorker)*IoWorkerPool{
	return &IoWorkerPool{
		queue:    list.New(),
		ioWorker: ioWorker,
		lock:     sync.RWMutex{},
		threadPoolShutDownFlag: atomic.NewBool(false),
	}
}

func (threadPool *IoWorkerPool) addTask(batch *ProductBatch) {
	defer threadPool.lock.Unlock()
	threadPool.lock.Lock()
	threadPool.queue.PushBack(batch)
}

func (threadPool *IoWorkerPool) hasTask() bool {
	defer threadPool.lock.RUnlock()
	threadPool.lock.RLock()
	return threadPool.queue.Len() > 0
}

func (threadPool *IoWorkerPool) popTask() *ProductBatch {
	defer threadPool.lock.Unlock()
	threadPool.lock.Lock()
	ele := threadPool.queue.Front()
	threadPool.queue.Remove(ele)
	return ele.Value.(*ProductBatch)
}

func(threadPool *IoWorkerPool)start(ioWorkerWaitGroup *sync.WaitGroup, ioThreadPoolwait *sync.WaitGroup){
	defer ioThreadPoolwait.Done()
	for {
		if threadPool.hasTask() {
			select {
			case threadPool.ioWorker.maxIoWorker <- 1:
				ioWorkerWaitGroup.Add(1)
				go threadPool.ioWorker.sendToServer(threadPool.popTask(), ioWorkerWaitGroup)
			}
		} else {
			if !threadPool.threadPoolShutDownFlag.Load() {
				time.Sleep(500 * time.Millisecond)
			} else {
				//level.Info(threadPool.logger).Log("msg", "All cache tasks in the thread pool have been successfully sent")
				break
			}
		}
	}
}