package dao

import (
	"github.com/eeeeeeeee-ccc/jt/dao/file"
	"github.com/eeeeeeeee-ccc/jt/dao/mongo"
	jkinterface "github.com/eeeeeeeee-ccc/jt/interface"
)

var DaoEnglineMap = map[string]func(ExtMap map[string]string) jkinterface.ProductClientInterface{
	"mongo":mongo.New,
	"file":file.New,
}



func ChooseEngine(ch string,ExtMap map[string]string) jkinterface.ProductClientInterface {
	if channel, ok := DaoEnglineMap[ch]; !ok {
		return nil
	} else {
		return channel(ExtMap)
	}
}
