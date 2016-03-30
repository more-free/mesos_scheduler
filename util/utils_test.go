package util

import (
	"github.com/more-free/mesos_scheduler/protocol"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostPriorityQueue(t *testing.T) {
	p1 := &protocol.Update{
		"id1",
		&protocol.Post{
			Cmd:       "p1",
			StartTime: 100,
		},
	}

	p2 := &protocol.Update{
		"id2",
		&protocol.Post{
			Cmd:       "p2",
			StartTime: 200,
		},
	}

	p3 := &protocol.Update{
		"id3",
		&protocol.Post{
			Cmd:       "p3",
			StartTime: 300,
		},
	}

	p4 := &protocol.Update{
		"id4",
		&protocol.Post{
			Cmd:       "p4",
			StartTime: 400,
		},
	}

	pq := NewPostPriorityQueue()
	pq.Push(p4)
	pq.Push(p2)
	pq.Push(p3)
	pq.Push(p1)

	all := pq.GetAll()
	assert.Equal(t, 4, len(all))

	assert.Equal(t, 4, pq.Len())
	top := pq.Pop()
	assert.Equal(t, p1, top)
	assert.Equal(t, 3, pq.Len())
	top = pq.Pop()
	assert.Equal(t, p2, top)
	assert.Equal(t, 2, pq.Len())

	all = pq.GetAll()
	assert.Equal(t, 2, len(all))

	top = pq.Pop()
	assert.Equal(t, p3, top)
	assert.Equal(t, 1, pq.Len())
	top = pq.Pop()
	assert.Equal(t, p4, top)
	top = pq.Pop()
	assert.Equal(t, (*protocol.Update)(nil), top)
	assert.Equal(t, 0, pq.Len())

	all = pq.GetAll()
	assert.Equal(t, 0, len(all))
}
