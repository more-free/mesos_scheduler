package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
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

type PortMapping struct {
	HostPort      uint32 `json:"host"`
	ContainerPort uint32 `json:"container"`
	Protocol      string `json:"protocol"`
}

// meta data about a task to schedule. poor name ?
type Post struct {
	StartTime    int64          `json:"start"`       // start time in seconds
	RepeatPeriod int64          `json:"repeat"`      // repeat period in seconds (NOT supported for now)
	MaxRetry     int32          `json:"retry"`       // how many failures in total it can tolerate, a negative value indicates infinite retry
	Cpu          float64        `json:"cpu"`         // virtual CPU usage
	Mem          float64        `json:"mem"`         // memory in MB
	Disk         float64        `json:"disk"`        // disk in MB. optional field
	PortMapping  []*PortMapping `json:"portmapping"` // host -> container port mappings, set host port to 0 indicates a dynamic port mapping
	Cmd          string         `json:"cmd"`         // shell or docker command
	CmdType      CmdType        `json:"cmdtype"`     // "shell" or "docker"
	Args         []string       `json:"args"`        // docker arguments. note that it will be ignored for shell command. optional field
	Image        string         `json:"image"`       // name of docker image. optional field
	Name         string         `json:"name"`        // human-readable task name. optional field
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

type TaskRunTimeList []*TaskRunTime

type TaskRunTime struct {
	Failure        int32          `json:"fail"`  // how many times it fails so far
	State          TaskState      `json:"state"` // current state
	Host           string         `json:"host"`  // on which mesos slave it runs
	PortMapping    []*PortMapping `json:"port"`  // required for dynamic port mapping
	LastModifiedMS int64          `json:"time"`  // milliseconds
	Post           *Post          `json:"post"`  // optional. meta data. for stat store only.
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

func (u *Update) ToDelete() *Delete {
	return &Delete{
		u.TaskId,
	}
}

func (u *Update) ToGet() *Get {
	return &Get{u.TaskId}
}

func (t *TaskRunTime) Copy() *TaskRunTime {
	return &TaskRunTime{
		Failure:        t.Failure,
		State:          t.State,
		Host:           t.Host,
		PortMapping:    t.PortMapping,
		LastModifiedMS: t.LastModifiedMS,
	}
}

func (t *TaskRunTime) WithFailure(failure int32) *TaskRunTime {
	t.Failure = failure
	return t
}

func (t *TaskRunTime) WithState(s TaskState) *TaskRunTime {
	t.State = s
	return t
}

func (t *TaskRunTime) WithHost(h string) *TaskRunTime {
	t.Host = h
	return t
}

func (t *TaskRunTime) WithPortMapping(p []*PortMapping) *TaskRunTime {
	t.PortMapping = p
	return t
}

func (t *TaskRunTime) WithLastModifiedMS(ts int64) *TaskRunTime {
	t.LastModifiedMS = ts
	return t
}

func (t *TaskRunTime) WithPost(p *Post) *TaskRunTime {
	t.Post = p
	return t
}

func (t *TaskRunTime) String() string {
	s := fmt.Sprintf("state=%v,failure=%v,host=%v,port=%v,lastmodified=%v",
		t.State, t.Failure, t.Host, t.PortMapping, t.LastModifiedMS)
	if t.Post != nil {
		s = fmt.Sprintf("%s,task=%v.", s, t.Post)
	}

	if bytes, err := json.Marshal(t); err == nil {
		return string(bytes)
	} else {
		return s
	}
}

func (p *PortMapping) String() string {
	if bytes, err := json.Marshal(p); err == nil {
		return string(bytes)
	} else {
		return fmt.Sprintf("%vhost:%vcontainer:%v", p.HostPort, p.ContainerPort, p.Protocol)
	}
}

func (t TaskState) String() string {
	switch t {
	case TASK_STATE_ERROR:
		return "error"
	case TASK_STATE_STAGING:
		return "staging"
	case TASK_STATE_STARTING:
		return "starting"
	case TASK_STATE_RUNNING:
		return "running"
	case TASK_STATE_FINISHED:
		return "finished"
	case TASK_STATE_FAILED:
		return "failed"
	default:
		return "unknown"
	}
}

func (t TaskRunTimeList) Len() int {
	return len(t)
}

func (t TaskRunTimeList) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t TaskRunTimeList) Less(i, j int) bool {
	return t[i].LastModifiedMS < t[j].LastModifiedMS
}
