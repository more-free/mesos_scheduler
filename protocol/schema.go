package protocol

import (
	"encoding/json"
)

type Path string
type CmdType string

const (
	CMD_SHELL  = CmdType("shell")
	CMD_DOCKER = CmdType("docker")
)

// meta data about a task to schedule. poor name ?
type Post struct {
	StartTime    int64    `json:"start"`   // start time in seconds
	RepeatPeriod int64    `json:"repeat"`  // repeat period in seconds
	MaxRetry     int      `json:"retry"`   // how many failures in total it can tolerate
	Cpu          float64  `json:"cpu"`     // virtual CPU usage
	Mem          float64  `json:"mem"`     // memory in MB
	Cmd          string   `json:"cmd"`     // shell or docker command
	CmdType      CmdType  `json:"cmdtype"` // "shell" or "docker"
	Args         []string `json:"args"`    // docker arguments. note that it will be ignored for shell command. optional field
	Image        string   `json:"image"`   // name of docker image. optional field
	Name         string   `json:"name"`    // human-readable task name. optional field
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
