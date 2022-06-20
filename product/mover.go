package product

import (
	"go.uber.org/atomic"
	"jk/util"
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
}

func initMover(collectionAccumulator *CollectionAccumulator, ioWorker *IoWorker, threadPool *IoWorkerPool) *Mover {
	mover := &Mover{
		moverShutDownFlag:     atomic.NewBool(false),
		ioWorker:              ioWorker,
		collectionAccumulator: collectionAccumulator,
		threadPool:            threadPool,
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
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
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
