package product

import (
	jkinterface "github.com/eeeeeeeee-ccc/jt/interface"
	"github.com/eeeeeeeee-ccc/jt/util"
	uberatomic "go.uber.org/atomic"
	"math"
	"sync"
	"time"
)

type IoWorker struct {
	client                 jkinterface.ProductClientInterface
	maxIoWorker            chan int64
	producer               *Product
	retryQueueShutDownFlag *uberatomic.Bool
	retryQueue             *RetryQueue
}

func initIoWorker(client jkinterface.ProductClientInterface, maxIoWorkerCount int64, producer *Product,retryQueue *RetryQueue ) *IoWorker {
	return &IoWorker{
		client:      client,
		maxIoWorker: make(chan int64, maxIoWorkerCount),
		producer:    producer,
		retryQueue:retryQueue,
	}
}

func (i *IoWorker) sendToServer(producerBatch *ProductBatch, ioWorkerWaitGroup *sync.WaitGroup) {
	if producerBatch == nil || ioWorkerWaitGroup == nil {
		return
	}
	//level.Debug(ioWorker.logger).Log("msg", "ioworker send data to server")
	defer i.closeSendTask(ioWorkerWaitGroup)
	err := i.client.PutCollection(producerBatch.getProject(), producerBatch.getSetName(), producerBatch.CollectionGroup, i.producer.productConfig.ExtMap)
	if err != nil {
		if i.retryQueueShutDownFlag.Load() {
			return
		}
		if producerBatch.attemptCount < producerBatch.maxRetryTimes {
			i.addErrorMessageToBatchAttempt(producerBatch)
			retryWaitTime := producerBatch.baseRetryBackoffMs * int64(math.Pow(2, float64(producerBatch.attemptCount)-1))
			if retryWaitTime < producerBatch.maxRetryIntervalInMs {
				producerBatch.nextRetryMs = util.GetTimeMs(time.Now().UnixNano()) + retryWaitTime
			} else {
				producerBatch.nextRetryMs = util.GetTimeMs(time.Now().UnixNano()) + producerBatch.maxRetryIntervalInMs
			}
			i.retryQueue.sendToRetryQueue(producerBatch)
		} else {
			//i.excuteFailedCallback(producerBatch)
			return
		}
	}
}

func (ioWorker *IoWorker) closeSendTask(ioWorkerWaitGroup *sync.WaitGroup) {
	ioWorkerWaitGroup.Done()
	//atomic.AddInt64(&ioWorker.taskCount, -1)
	<-ioWorker.maxIoWorker
	return
}

func (ioWorker *IoWorker) addErrorMessageToBatchAttempt(producerBatch *ProductBatch) {
	producerBatch.attemptCount += 1
	return
}
