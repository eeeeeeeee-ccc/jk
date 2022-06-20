package Kv

type Kv struct {
	Time    *string      `json:"string"`
	Content []*KvContent `json:"content"`
}

type KvContent struct {
	Key   *string `protobuf:"bytes,1,req,name=Key" json:"Key,omitempty"`
	Value *string `protobuf:"bytes,2,req,name=Value" json:"Value,omitempty"`
}

type CollectionGroup struct {
	Collections []*Kv   `json:"collections"`
	Topic       *string `protobuf:"bytes,3,opt,name=Topic" json:"Topic,omitempty"`
	Source      *string `protobuf:"bytes,4,opt,name=Source" json:"Source,omitempty"`
}

func (m *CollectionGroup) GetLogs() []*Kv {
	if m != nil {
		return m.Collections
	}
	return nil
}
