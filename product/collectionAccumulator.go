package product

import (
	uberatomic "go.uber.org/atomic"
	Kv "github.com/eeeeeeeee-ccc/jt/model/kv"
	"strings"
	"sync"
	"time"
)

type CollectionAccumulator struct {
	CollectionGroupData map[string]*ProductBatch `json:"collectionGroupData"`
	threadPool          *IoWorkerPool
	product             *Product
	productConfig       *ProductConfig
	lock                sync.RWMutex
	shutDownFlag        *uberatomic.Bool
}

func initCollectionAccumulator(ioWorkerPool *IoWorkerPool,product *Product,productConfig *ProductConfig ) *CollectionAccumulator{
	return &CollectionAccumulator{
		CollectionGroupData: make(map[string]*ProductBatch),
		threadPool:          ioWorkerPool,
		product:             product,
		productConfig:       productConfig,
		shutDownFlag:        uberatomic.NewBool(false),
	}
}

func (collectionAccumulator *CollectionAccumulator) AddCollectionBatch(project, setName string ,kv interface{}) {
	//获取批次号
	key := GetBatchNum()
	defer collectionAccumulator.lock.Unlock()
	collectionAccumulator.lock.Lock()
	if producerBatch, ok := collectionAccumulator.CollectionGroupData[key]; ok == true {
		collectionAccumulator.addOrSendProducerBatch(key, producerBatch, kv)
	}else{
		collectionAccumulator.createNewProducerBatch(kv,key,project,setName)
	}
}

func GetBatchNum() string {
	var build strings.Builder
	tim := time.Now().Format("01-01-1970")
	build.WriteString(tim)
	return build.String()
}

func (collectionAccumulator *CollectionAccumulator) addOrSendProducerBatch(key string, producerBatch *ProductBatch, kv interface{}) {
	if collectionAccumulator.shutDownFlag.Load() {
		//Producer has started and shut down and cannot write to new logs
		return
	}
	totalNum := producerBatch.getLogGroupCount() + 1
	if totalNum < collectionAccumulator.productConfig.MaxBatchCount {
		producerBatch.addLogToLogGroup(kv)
	} else {
		producerBatch.addLogToLogGroup(kv)
		collectionAccumulator.innerSendToServer(key, producerBatch)
	}
}

func (collectionAccumulator *CollectionAccumulator) innerSendToServer(key string, producerBatch *ProductBatch) {
	collectionAccumulator.threadPool.addTask(producerBatch)
	delete(collectionAccumulator.CollectionGroupData, key)
}


func (collectionAccumulator *CollectionAccumulator)createNewProducerBatch(kv interface{},key,project, setName string){
	if mkv, ok := kv.(*Kv.Kv); ok {
		newProducerBatch := initProducerBatch(mkv, project, setName)
		collectionAccumulator.CollectionGroupData[key] = newProducerBatch
	} else if mkvList, ok := kv.([]*Kv.Kv); ok {
		newProducerBatch := initProducerBatch(mkvList, project, setName)
		collectionAccumulator.CollectionGroupData[key] = newProducerBatch
	}
}

