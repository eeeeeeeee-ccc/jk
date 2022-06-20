package jkinterface

import Kv "jt/model/kv"

type ProductClientInterface interface {
	PutCollection(project, setName string,group *Kv.CollectionGroup,extMap map[string]string)
}