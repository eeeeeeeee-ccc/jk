package product

import (
	"jk/dao"
	jkinterface "jk/interface"
	"sync"
)



type Product struct {
	ioThreadPoolWaitGroup *sync.WaitGroup
	ioWorkerWaitGroup     *sync.WaitGroup
	moverWaitGroup        *sync.WaitGroup
	threadPool            *IoWorkerPool
	collectionAccumulator  *CollectionAccumulator
	//producerLogGroupSize  int64
	productConfig *ProductConfig
	mover                 *Mover
}

// injectionEngine  used  to open to developers to implement their own engine, it is not required
// To use, enable the injection engine in the producer's configuration
func InitProduct(productConfig *ProductConfig,engine string,injectionEngine jkinterface.ProductClientInterface)*Product{
	var client jkinterface.ProductClientInterface
	product:=&Product{
		productConfig:         productConfig,
	}
	if productConfig.IsInjectionEngine{
		client=injectionEngine
	}else{
		client=dao.ChooseEngine(engine,productConfig.ExtMap)
	}
	ioWorker:=initIoWorker(client,productConfig.MaxIoWorkerCount,product)
	threadPool:=initIoWorkerPool(ioWorker)

	collectionAccumulator:=initCollectionAccumulator(threadPool,product,productConfig)
	mover := initMover(collectionAccumulator, ioWorker,  threadPool)
	product.threadPool=threadPool
	product.collectionAccumulator=collectionAccumulator
	product.ioThreadPoolWaitGroup= &sync.WaitGroup{}
	product.ioWorkerWaitGroup=&sync.WaitGroup{}
	product.moverWaitGroup=&sync.WaitGroup{}
	product.mover=mover
	return product
}

func(p *Product)Start(){
	p.moverWaitGroup.Add(1)
	go p.mover.run(p.moverWaitGroup, p.productConfig)
	p.ioThreadPoolWaitGroup.Add(1)
	go p.threadPool.start(p.ioWorkerWaitGroup,p.ioThreadPoolWaitGroup)
}

func (producer *Product) SendCollection(project, setName string, collection interface{})  {
	 producer.collectionAccumulator.AddCollectionBatch(project, setName, collection)
	 return
}

func (producer *Product) SafeClose() {
	producer.sendCloseProdcerSignal()
	producer.moverWaitGroup.Wait()
	producer.threadPool.threadPoolShutDownFlag.Store(true)
	producer.ioThreadPoolWaitGroup.Wait()
	producer.ioWorkerWaitGroup.Wait()
}

func (producer *Product) sendCloseProdcerSignal() {
	producer.mover.moverShutDownFlag.Store(true)
	producer.collectionAccumulator.shutDownFlag.Store(true)
}


