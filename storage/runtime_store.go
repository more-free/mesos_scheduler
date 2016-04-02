package storage

import (
	"github.com/more-free/mesos_scheduler/protocol"
	"github.com/more-free/mesos_scheduler/util"
)

type TaskId string

type RuntimeStore interface {
	GetRuntime(TaskId) (*protocol.TaskRunTime, error)
	// multi-use function.  it returns valid path (string) if the taskId doesn't exist, otherwise it returns the old runtime.
	// in simple implementation the returned path could be the same as the input TaskId.
	SetRuntime(TaskId, *protocol.TaskRunTime) (string, *protocol.TaskRunTime, error)

	GetState(TaskId) (protocol.TaskState, error)
	SetState(TaskId, protocol.TaskState) (protocol.TaskState, error) // return old state

	GetFailure(TaskId) (int32, error)
	SetFailure(TaskId, int32) (int32, error) // return old failure

	Delete(TaskId) error

	GetAllId() ([]TaskId, error)
	DeleteAll() error

	Open() error
	Close() error
}

type ZkRuntimeStore struct {
	*ZkStorage
}

func NewZkRuntimeStore(zkStore *ZkStorage) RuntimeStore {
	return &ZkRuntimeStore{zkStore}
}

func (zk *ZkRuntimeStore) GetRuntime(id TaskId) (*protocol.TaskRunTime, error) {
	bytes, _, err := zk.conn.Get(zk.getPath(string(id)))
	if err != nil {
		return nil, err
	}

	rt, err := protocol.ToTaskRunTime(bytes)
	if err != nil {
		return nil, err
	} else {
		return rt, nil
	}
}

func (zk *ZkRuntimeStore) SetRuntime(id TaskId, nrt *protocol.TaskRunTime) (string, *protocol.TaskRunTime, error) {
	nrt = nrt.WithLastModifiedMS(util.NowInMS())
	exists, _, err := zk.conn.Exists(zk.getPath(string(id)))
	if err != nil {
		return "", nil, err
	}
	if !exists {
		data, _ := protocol.ToBytes(nrt)
		path, err := zk.conn.Create(zk.getPath(string(id)), data, zk.flags, zk.acl)
		if err != nil {
			return "", nil, err
		} else {
			return path, nrt, nil
		}
	} else {
		ort, err := zk.GetRuntime(id)
		if err != nil {
			return "", nil, err
		}
		data, _ := protocol.ToBytes(nrt)
		_, err = zk.conn.Set(zk.getPath(string(id)), data, -1)
		if err != nil {
			return "", nil, err
		} else {
			return "", ort, nil
		}
	}
}

func (zk *ZkRuntimeStore) GetState(id TaskId) (protocol.TaskState, error) {
	rt, err := zk.GetRuntime(id)
	if err != nil {
		return protocol.TASK_STATE_ERROR, err
	} else {
		return rt.State, nil
	}
}

func (zk *ZkRuntimeStore) SetState(id TaskId, ns protocol.TaskState) (protocol.TaskState, error) {
	rt, err := zk.GetRuntime(id)
	if err != nil {
		return protocol.TASK_STATE_ERROR, err
	}

	_, _, err = zk.SetRuntime(id, rt.WithState(ns))
	if err != nil {
		return protocol.TASK_STATE_ERROR, err
	}

	return rt.State, nil
}

func (zk *ZkRuntimeStore) GetFailure(id TaskId) (int32, error) {
	rt, err := zk.GetRuntime(id)
	if err != nil {
		return -1, err
	} else {
		return rt.Failure, nil
	}
}

func (zk *ZkRuntimeStore) SetFailure(id TaskId, f int32) (int32, error) {
	rt, err := zk.GetRuntime(id)
	if err != nil {
		return -1, err
	}

	_, _, err = zk.SetRuntime(id, rt.WithFailure(f))
	if err != nil {
		return -1, err
	}

	return rt.Failure, nil
}

func (zk *ZkRuntimeStore) Delete(id TaskId) error {
	return zk.conn.Delete(zk.getPath(string(id)), -1)
}

func (zk *ZkRuntimeStore) GetAllId() ([]TaskId, error) {
	ids, _, err := zk.conn.Children(zk.rootDir)
	taskIds := make([]TaskId, len(ids))
	for i := 0; i < len(ids); i++ {
		taskIds[i] = TaskId(ids[i])
	}
	return taskIds, err
}

func (zk *ZkRuntimeStore) DeleteAll() error {
	ids, err := zk.GetAllId()
	if err != nil {
		return err
	}

	for _, id := range ids {
		err = zk.Delete(id)
		if err != nil {
			return err
		}
	}
	return nil
}
