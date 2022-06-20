package jkinterface

import Kv "jk/model/kv"

type ProductClientInterface interface {
	PutCollection(project, setName string,group *Kv.CollectionGroup,extMap map[string]string)
}