package file

import (
	jkinterface "github.com/eeeeeeeee-ccc/jt/interface"
	Kv "github.com/eeeeeeeee-ccc/jt/model/kv"
	"os"
	"strings"
)

type fL struct {
}

func New(ExtMap map[string]string)jkinterface.ProductClientInterface{
	return new(fL)
}

func (f *fL) PutCollection(project, setName string, group *Kv.CollectionGroup,extMap map[string]string)error {
	var path string
	path =extMap["path"]
	if len(group.Collections) == 0 {
		return nil
	}
	subArr := []string{}
	for _, item := range group.Collections {
		var e string
		for _, akv := range item.Content {
			e += *akv.Key + ":" + *akv.Value
		}
		subArr = append(subArr, e)
	}
	fileContent:=strings.Join(subArr,"\n")+"\n"
	fileD, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil
	}
	defer fileD.Close()
	_, err = fileD.Write([]byte(fileContent))
	if err != nil {
		panic(err)
	}
	return err
}
