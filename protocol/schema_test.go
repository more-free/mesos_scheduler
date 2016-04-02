package protocol

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSchema(t *testing.T) {
	post := &Post{
		StartTime:    1000,
		RepeatPeriod: 2000,
		Cpu:          10.0,
		Mem:          20.0,
		Cmd:          "echo test",
	}

	bytes, err := ToBytes(post)
	assert.Equal(t, nil, err)

	update := &Update{
		TaskId: "update-id",
		Post:   post,
	}

	bytes, err = ToBytes(update)
	assert.Equal(t, nil, err)

	newUpdate, _ := ToUpdate(bytes)
	assert.Equal(t, newUpdate.Cpu, update.Cpu)
	assert.Equal(t, newUpdate.Mem, update.Mem)
	assert.Equal(t, newUpdate.RepeatPeriod, update.RepeatPeriod)
	assert.Equal(t, newUpdate.StartTime, update.StartTime)
	assert.Equal(t, newUpdate.TaskId, update.TaskId)
}
