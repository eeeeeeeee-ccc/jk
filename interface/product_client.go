package jkinterface

import Kv "github.com/eeeeeeeee-ccc/jt/model/kv"

type ProductClientInterface interface {
	PutCollection(project, setName string,group *Kv.CollectionGroup,extMap map[string]string)
}