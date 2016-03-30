package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

type Path string
type CmdType string
type TaskState int32

const (
	CMD_SHELL  = CmdType("shell")
	CMD_DOCKER = CmdType("docker")
)

const (
	TASK_STATE_ERROR    = TaskState(-1)
	TASK_STATE_STAGING  = TaskState(0)
	TASK_STATE_STARTING = TaskState(1)
	TASK_STATE_RUNNING  = TaskState(2)
	TASK_STATE_FINISHED = TaskState(3)
	TASK_STATE_FAILED   = TaskState(4)
)

// meta data about a task to schedule. poor name ?
type Post struct {
	StartTime    int64    `json:"start"`   // start time in seconds
	RepeatPeriod int64    `json:"repeat"`  // repeat period in seconds
	MaxRetry     int32    `json:"retry"`   // how many failures in total it can tolerate, a negative value indicates infinite retry
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

type TaskRunTime struct {
	Failure        int32     `json:"fail"`
	State          TaskState `json:"state"`
	LastModifiedMS int64     `json:"time"` // milliseconds
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

func (s TaskState) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, int32(s))
	if err != nil {
		return make([]byte, 0), err
	} else {
		return buf.Bytes(), nil
	}
}

func ToTaskState(data []byte) (TaskState, error) {
	buf := bytes.NewBuffer(data)
	var s int32
	err := binary.Read(buf, binary.LittleEndian, &s)
	if err != nil {
		return TASK_STATE_ERROR, err
	} else {
		return TaskState(s), nil
	}
}

func ToTaskRunTime(data []byte) (*TaskRunTime, error) {
	var trt TaskRunTime
	err := json.Unmarshal(data, &trt)
	if err != nil {
		return nil, err
	} else {
		return &trt, nil
	}
}
