package test

import (
	"fmt"
	"io/ioutil"
	jkinterface "jt/interface"
	Kv "jt/model/kv"
	"jt/product"
	"os"
	"os/signal"
	"strings"
	"testing"
)

func GetSubArr() *Kv.Kv{
	var contentArr=[]*Kv.KvContent{}
	var engineKey,engineName=new(string),new(string)
	var versionKey,versionVal=new(string),new(string)
	var writerKey,writerVal=new(string),new(string)
	var famousSentenceKey,famouSentence=new(string),new(string)
	var timeKey,timeVal= new(string),new(string)
	var locationKey,locationVal= new(string),new(string)
	var tKey=new(string)
	*engineKey="engine"
	*engineName="mongo"
   content:=&Kv.KvContent{
	   Key:   engineKey,
	   Value: engineName,
   }
   contentArr=append(contentArr,content)
	*versionKey="version"
	*versionVal="v1.1.1"
	content =&Kv.KvContent{
		Key:   versionKey,
		Value: versionVal,
	}
	contentArr=append(contentArr,content)
	*writerKey="作者"
	*writerVal="张麻子"
	content =&Kv.KvContent{
		Key:   writerKey,
		Value: writerVal,
	}
	contentArr=append(contentArr,content)
	*famousSentenceKey="名言"
	*famouSentence="乱拳打晕老师傅"
	content =&Kv.KvContent{
		Key:   famousSentenceKey,
		Value: famouSentence,
	}
	contentArr=append(contentArr,content)
	*timeKey="北京时间"
	*timeVal="壬寅年 丙午月 己亥日,未时末"
	content =&Kv.KvContent{
		Key:   timeKey,
		Value: timeVal,
	}
	contentArr=append(contentArr,content)
	*locationKey="地点"
	*locationVal="南国鹅城"
	content =&Kv.KvContent{
		Key:   locationKey,
		Value: locationVal,
	}
	contentArr=append(contentArr,content)
	*tKey="壬寅年 丙午月 己亥日"
	kv := &Kv.Kv{
		Time:   tKey ,
		Content: contentArr,
	}
	return kv
}

func TestInsert(t *testing.T) {
	var (
		Product *product.Product
	)
	extMap:=make(map[string]string)
	extMap["path"]="test.text"
	ch := make(chan os.Signal)
	signal.Notify(ch)
	productConfig := product.GetDefaultProductConfig()
	productConfig.ExtMap=extMap
	producter := product.InitProduct(productConfig, "file",nil)
	producter.Start()
	Product = producter
	result:=GetSubArr()
	Product.SendCollection("HallofFame","celebrityBiography",result)
	for i := 0; i < 5; i++ {
		go func(id int) {
			for i := 0; i < 10; i++ {
				Product.SendCollection("project", "logstore",  GetSubArr())
			}
			fmt.Println("All data in the queue has been sent, goroutine id:", id)
		}(i)
	}
	if _, ok := <-ch; ok {
		Product.SafeClose()
		fmt.Println("Get the shutdown signal and start to shut down")
	}
}

func TestInsertMongo(t *testing.T) {
	var (
		Product *product.Product
	)
	extMap:=make(map[string]string)
	extMap["mongodb_connect_info"]="mongodb://127.0.0.1:27017"
	ch := make(chan os.Signal)
	signal.Notify(ch)
	productConfig := product.GetDefaultProductConfig()
	productConfig.ExtMap=extMap
	producter := product.InitProduct(productConfig, "mongo",nil)
	producter.Start()
	Product = producter
	for i := 0; i < 5; i++ {
		go func(id int) {
			for i := 0; i < 4; i++ {
				Product.SendCollection("HallofFame", "celebrityBiography",  GetSubArr())
			}
			fmt.Println("All data in the queue has been sent, goroutine id:", id)
		}(i)
	}
	if _, ok := <-ch; ok {
		Product.SafeClose()
		fmt.Println("Get the shutdown signal and start to shut down")
	}
}

//注入引擎
func TestInsertInjectionEngine(t *testing.T) {
	var (
		Product *product.Product
	)
	extMap:=make(map[string]string)
	extMap["path"]="test.text"
	ch := make(chan os.Signal)
	signal.Notify(ch)
	productConfig := product.GetDefaultProductConfig()
	productConfig.ExtMap=extMap
	productConfig.IsInjectionEngine=true
	flEngine:=New()
	producter := product.InitProduct(productConfig, "",flEngine)
	producter.Start()
	Product = producter
	for i := 0; i < 5; i++ {
		go func(id int) {
			for i := 0; i < 4; i++ {
				Product.SendCollection("HallofFame", "celebrityBiography",  GetSubArr())
			}
			fmt.Println("All data in the queue has been sent, goroutine id:", id)
		}(i)
	}
	if _, ok := <-ch; ok {
		Product.SafeClose()
		fmt.Println("Get the shutdown signal and start to shut down")
	}
}


func TestAc(t *testing.T){
	ch := make(chan os.Signal)
	signal.Notify(ch)
	if _, ok := <-ch; ok {
		fmt.Println("Get the shutdown signal and start to shut down")
	}
}

func TestWriteFile(t *testing.T){
	var  at,bt,ct=new(string),new(string),new(string)
	var  ak,bk,ck,av,bv,cv=new(string),new(string),new(string),new(string),new(string),new(string)
	*at,*bt,*ct="1","2","3"
	*ak,*bk,*ck,*av,*bv,*cv="ak","bk","ck","av","bv","cv"
	k:=[]*Kv.Kv{{
		Time:   at,
		Content: []*Kv.KvContent{{
			Key:   ak,
			Value: av,
		}},
	},{
		Time:    bt,
		Content: []*Kv.KvContent{{
			Key:   bk,
			Value: bv,
		}},
	},{
		Time:    ct,
		Content: []*Kv.KvContent{{
			Key:   ck,
			Value: cv,
		}},
	}}
	f:=Kv.CollectionGroup{
		Collections: k,
		Topic:       nil,
		Source:      nil,
	}

	subArr := []string{}
	for _, item := range f.Collections {
		var e string
		for _, akv := range item.Content {
			e += *akv.Key + ":" + *akv.Value
		}
		subArr = append(subArr, e)
	}
	ov:=strings.Join(subArr,"\n")
	err:=ioutil.WriteFile("test.text",[]byte(ov),0664)
	if err != nil {
		panic(err)
	}
}

type fL struct {
}

func New()jkinterface.ProductClientInterface{
	return new(fL)
}

func (f *fL) PutCollection(project, setName string, group *Kv.CollectionGroup,extMap map[string]string) {
	var path string
	path =extMap["path"]
	if len(group.Collections) == 0 {
		// empty log group
		return
	}
	subArr := []string{}
	for _, item := range group.Collections {
		var e string
		for _, akv := range item.Content {
			e += *akv.Key + ":" + *akv.Value
		}
		subArr = append(subArr, e)
	}
	b:=strings.Join(subArr,"\n")+"\n"
	fl, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	defer fl.Close()
	_, err = fl.Write([]byte(b))
	if err != nil {
		panic(err)
	}
	return
}
