package storage

import (
	"github.com/more-free/mesos_scheduler/protocol"
	"github.com/stretchr/testify/assert"
	"log"
	"os/exec"
	"strings"
	"testing"
)

func TestRuntimeStore(t *testing.T) {
	out, err := exec.Command("bash", "-c", "echo ruok | nc localhost 2181").Output()
	if err != nil || string(out) != "imok" {
		log.Println("zookeeper is not running on localhost:2181. Pass the test")
		return
	}
	log.Println("Testing runtime store")
	servers := strings.Split("localhost:2181", ",")
	s := NewZkStorage(servers, "/test-scheduler/data")
	ss := NewZkRuntimeStore(s.(*ZkStorage))
	err = ss.Open()
	defer ss.Close()
	assert.Equal(t, nil, err)

	// get from empty
	id := TaskId("some")
	rt, err := ss.GetRuntime(id)
	assert.NotNil(t, err)
	assert.Nil(t, rt)

	// set
	n := &protocol.TaskRunTime{
		3,
		protocol.TASK_STATE_STAGING,
	}
	path, rt, err := ss.SetRuntime(id, n)
	assert.Equal(t, n, rt)
	assert.Equal(t, nil, err)
	assert.NotEmpty(t, path)

	// update
	nn := &protocol.TaskRunTime{
		4,
		protocol.TASK_STATE_STARTING,
	}
	path, rt, err = ss.SetRuntime(id, nn)
	assert.Equal(t, n, rt) // should be equal to old state
	assert.Equal(t, nil, err)
	assert.Empty(t, path)

	// get again
	rt, err = ss.GetRuntime(id)
	assert.Equal(t, nn, rt)
	assert.Equal(t, nil, err)

	// delete
	err = ss.Delete(id)
	assert.Equal(t, nil, err)

	// get after delete
	rt, err = ss.GetRuntime(id)
	assert.Nil(t, rt)
	assert.NotNil(t, err)

}
