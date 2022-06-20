package file

import (
	jkinterface "jk/interface"
	Kv "jk/model/kv"
	"os"
	"strings"
)

type fL struct {
}

func New(ExtMap map[string]string)jkinterface.ProductClientInterface{
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
