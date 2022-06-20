package product

import (
	"sync"
	"jk/model/kv"
	"time"
)

type ProductBatch struct {
	CollectionGroup *Kv.CollectionGroup `json:"collection_group"`
	TotalNum        int64            //最多10个
	lock            sync.RWMutex
	project         string
	set             string
	createTimeMs    int64
}



func initProducerBatch(kv interface{},project, setName string)*ProductBatch{
	collections := []*Kv.Kv{}
	if log, ok := kv.(*Kv.Kv); ok {
		collections = append(collections, log)
	} else if logList, ok := kv.([]*Kv.Kv); ok {
		collections = append(collections, logList...)
	}
	collectionGroup :=&Kv.CollectionGroup{
		Collections:   collections,
	}
	productBatch := &ProductBatch{
		CollectionGroup: collectionGroup,
		TotalNum:        10,
		project:         project,
		set:             setName,
		createTimeMs:    time.Now().UnixNano()/1e6,
	}
	return productBatch
}

func (productBatch *ProductBatch) getLogGroupCount() int {
	defer productBatch.lock.RUnlock()
	productBatch.lock.RLock()
	return len(productBatch.CollectionGroup.GetLogs())
}


func (productBatch *ProductBatch) addLogToLogGroup(kv interface{}) {
	defer productBatch.lock.Unlock()
	productBatch.lock.Lock()
	if mkv, ok := kv.(*Kv.Kv); ok {
		productBatch.CollectionGroup.Collections = append(productBatch.CollectionGroup.Collections, mkv)
	} else if mkvList, ok := kv.([]*Kv.Kv); ok {
		productBatch.CollectionGroup.Collections = append(productBatch.CollectionGroup.Collections, mkvList...)
	}
}

func (producerBatch *ProductBatch) getProject() string {
	defer producerBatch.lock.RUnlock()
	producerBatch.lock.RLock()
	return producerBatch.project
}

func (producerBatch *ProductBatch) getSetName() string {
	defer producerBatch.lock.RUnlock()
	producerBatch.lock.RLock()
	return producerBatch.set
}