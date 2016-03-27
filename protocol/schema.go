package protocol

import "encoding/json"

type Path string

type Post struct {
	StartTime uint64 `json:"start"`
	RepeatPeriod uint64 `json:"repeat"`
	Cpu float64	`json:"cpu"`
	Mem float64 `json:"mem"`
	Cmd string  `json:"cmd"`
}

type Update struct {
	TaskId string `json:"id"`
	*Post
}

type Get struct {
	TaskId string `json:"id"`
}

type Delete struct {
	TaskId string `json:"id"`
}

func ToBytes(p interface{}) ([]byte, error) {
	return json.Marshal(p)
}

func ToPost(bytes []byte) (*Post, error) {
	var post Post
	err := json.Unmarshal(bytes, &post)
	if err != nil {
		return nil, err
	} else {
		return &post, nil
	}
}

func ToUpdate(bytes []byte) (*Update, error) {
	var update Update
	err := json.Unmarshal(bytes, &update)
	if err != nil {
		return nil, err
	} else {
		return &update, nil
	}
}

func ToGet(bytes []byte) (*Get, error) {
	var get Get
	err := json.Unmarshal(bytes, &get)
	if err != nil {
		return nil, err
	} else {
		return &get, nil
	}
}

func ToDelete(bytes []byte) (*Delete, error) {
	var del Delete
	err := json.Unmarshal(bytes, &del)
	if err != nil {
		return nil, err
	} else {
		return &del, nil
	}
}

