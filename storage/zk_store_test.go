package storage

import (
	"github.com/more-free/mesos_scheduler/protocol"
	"github.com/stretchr/testify/assert"
	"log"
	"os/exec"
	"strings"
	"testing"
)

func TestZkStorage(t *testing.T) {
	out, err := exec.Command("bash", "-c", "echo ruok | nc localhost 2181").Output()
	if err != nil || string(out) != "imok" {
		log.Println("zookeeper is not running on localhost:2181. Pass the test")
		return
	}

	servers := strings.Split("localhost:2181", ",")
	zk := NewZkStorage(servers, "/test-scheduler/data")
	err = zk.Open()
	defer zk.Close()
	assert.Equal(t, nil, err)

	post := &protocol.Post{
		StartTime:    1000,
		RepeatPeriod: 2000,
		Cpu:          10.0,
		Mem:          20.0,
		Cmd:          "post",
	}

	id, err := zk.Create(post)
	assert.Equal(t, nil, err)

	post.Cmd = "update"
	update := &protocol.Update{
		id, post,
	}
	err = zk.Update(update)
	assert.Equal(t, nil, err)

	get := &protocol.Get{
		TaskId: id,
	}
	bytes, err := zk.Get(get)
	assert.Equal(t, nil, err)

	data, err := protocol.ToPost(bytes)
	assert.Equal(t, nil, err)
	assert.Equal(t, "update", data.Cmd)

	children, err := zk.GetAll()
	assert.Equal(t, nil, err)
	for _, id := range children {
		get = &protocol.Get{id}
		_, err = zk.Get(get)
		assert.Equal(t, nil, err)
	}

	del := &protocol.Delete{
		TaskId: id,
	}
	err = zk.Delete(del)
	assert.Equal(t, nil, err)

	err = zk.DeleteAll()
	assert.Equal(t, nil, err)
}
