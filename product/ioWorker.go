package product

import (
	jkinterface "jt/interface"
	"sync"
)

type IoWorker struct {
	client      jkinterface.ProductClientInterface
	maxIoWorker chan int64
	producer       *Product
}

func initIoWorker(client jkinterface.ProductClientInterface,maxIoWorkerCount int64,producer *Product) *IoWorker {
	return &IoWorker{
		client:      client,
		maxIoWorker: make(chan int64,maxIoWorkerCount),
		producer:   producer,
	}
}


func(i *IoWorker)sendToServer(producerBatch *ProductBatch, ioWorkerWaitGroup *sync.WaitGroup){
	if producerBatch == nil || ioWorkerWaitGroup == nil {
		return
	}
	//level.Debug(ioWorker.logger).Log("msg", "ioworker send data to server")
	defer i.closeSendTask(ioWorkerWaitGroup)
	i.client.PutCollection(producerBatch.getProject(),producerBatch.getSetName(),producerBatch.CollectionGroup,i.producer.productConfig.ExtMap)
}

func (ioWorker *IoWorker) closeSendTask(ioWorkerWaitGroup *sync.WaitGroup) {

	ioWorkerWaitGroup.Done()
	//atomic.AddInt64(&ioWorker.taskCount, -1)
	<-ioWorker.maxIoWorker
}