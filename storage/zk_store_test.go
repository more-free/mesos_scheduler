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
	log.Println("Testing zookeeper store")
	servers := strings.Split("localhost:2181", ",")
	zk := NewZkStorage(servers, "/test-scheduler/state")
	err = zk.Open()
	defer zk.Close()
	assert.Equal(t, nil, err)
	zk.DeleteAll()

	post := &protocol.Post{
		StartTime:    1000,
		RepeatPeriod: 2000,
		Cpu:          10.0,
		Mem:          20.0,
		Cmd:          "post",
	}

	id, err := zk.Post(post)
	assert.Equal(t, nil, err)

	post.Cmd = "update"
	update := &protocol.Update{
		id.TaskId, post,
	}
	err = zk.Update(update)
	assert.Equal(t, nil, err)

	get := &protocol.Get{
		TaskId: id.TaskId,
	}
	data, err := zk.Get(get)
	assert.Equal(t, nil, err)
	assert.Equal(t, "update", data.Cmd)

	datas, err := zk.GetAll()
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(datas))

	// create again
	_, err = zk.Post(post)
	assert.Equal(t, nil, err)
	datas, err = zk.GetAll()
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(datas))
	for i := 0; i < len(datas); i++ {
		assert.Equal(t, post, datas[i].Post)
	}

	del := &protocol.Delete{
		TaskId: id.TaskId,
	}
	err = zk.Delete(del)
	assert.Equal(t, nil, err)

	err = zk.DeleteAll()
	assert.Equal(t, nil, err)

	datas, err = zk.GetAll()
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(datas))
}
