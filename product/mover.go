package product

import (
	"github.com/eeeeeeeee-ccc/jt/util"
	"go.uber.org/atomic"
	"log"
	"sync"
	"time"
)

type Mover struct {
	moverShutDownFlag     *atomic.Bool
	ioWorker              *IoWorker
	collectionAccumulator *CollectionAccumulator
	logger                log.Logger
	threadPool            *IoWorkerPool
	retryQueue            *RetryQueue
}

func initMover(collectionAccumulator *CollectionAccumulator, ioWorker *IoWorker, threadPool *IoWorkerPool, retryQueue *RetryQueue) *Mover {
	mover := &Mover{
		moverShutDownFlag:     atomic.NewBool(false),
		ioWorker:              ioWorker,
		collectionAccumulator: collectionAccumulator,
		threadPool:            threadPool,
		retryQueue:            retryQueue,
	}
	return mover
}

func (mover *Mover) run(moverWaitGroup *sync.WaitGroup, config *ProductConfig) {
	defer moverWaitGroup.Done()
	for !mover.moverShutDownFlag.Load() {
		sleepMs := config.LingerMs
		nowTimeMs := (time.Now().UnixNano()) / 1e6
		mover.collectionAccumulator.lock.Lock()
		mapCount := len(mover.collectionAccumulator.CollectionGroupData)
		for key, batch := range mover.collectionAccumulator.CollectionGroupData {
			timeInterval := batch.createTimeMs + config.LingerMs - nowTimeMs
			if timeInterval <= 0 {
				//level.Debug(mover.logger).Log("msg", "mover groutine execute sent producerBatch to IoWorker")
				mover.sendToServer(key, batch, config)
			} else {
				if sleepMs > timeInterval {
					sleepMs = timeInterval
				}
			}
		}
		mover.collectionAccumulator.lock.Unlock()
		if mapCount == 0 {
			//level.Debug(mover.logger).Log("msg", "No data time in map waiting for user configured RemainMs parameter values")
			sleepMs = config.LingerMs
		}
		retryProducerBatchList := mover.retryQueue.getRetryBatch(mover.moverShutDownFlag.Load())
		if retryProducerBatchList == nil {
			// If there is nothing to send in the retry queue, just wait for the minimum time that was given to me last time.
			time.Sleep(time.Duration(sleepMs) * time.Millisecond)
		} else {
			count := len(retryProducerBatchList)
			for i := 0; i < count; i++ {
				mover.threadPool.addTask(retryProducerBatchList[i])
			}
		}
	}
	mover.collectionAccumulator.lock.Lock()
	for _, batch := range mover.collectionAccumulator.CollectionGroupData {
		mover.threadPool.addTask(batch)
	}
	mover.collectionAccumulator.CollectionGroupData = make(map[string]*ProductBatch)
	mover.collectionAccumulator.lock.Unlock()
	//level.Info(mover.logger).Log("msg", "mover thread closure complete")
}

func (mover *Mover) sendToServer(key string, batch *ProductBatch, config *ProductConfig) {
	if value, ok := mover.collectionAccumulator.CollectionGroupData[key]; !ok {
		return
	} else if util.GetTimeMs(time.Now().UnixNano())-value.createTimeMs < config.LingerMs {
		return
	}
	mover.threadPool.addTask(batch)
	delete(mover.collectionAccumulator.CollectionGroupData, key)
}
