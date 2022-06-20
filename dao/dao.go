package dao

import (
	"jt/dao/file"
	"jt/dao/mongo"
	jkinterface "jt/interface"
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
