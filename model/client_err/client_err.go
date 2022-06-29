package client_err

import "encoding/json"

type Error struct {
	HttpCode int    `json:"http_code"`
	Code     int    `json:"code"`
	Msg      string `json:"msg"`
}

func (e Error)String ()string{
     b,_:=json.Marshal(e)
     return string(b)
}


func (e Error)Error ()string{
	return e.String()
}