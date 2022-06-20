package dao

import (
	"jk/dao/file"
	"jk/dao/mongo"
	jkinterface "jk/interface"
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
