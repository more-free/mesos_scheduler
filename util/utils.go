package util

import (
	"container/heap"
	"github.com/more-free/mesos_scheduler/protocol"
)

type PriorityQueue []*protocol.Update

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].StartTime < pq[j].StartTime
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	*pq = append(*pq, x.(*protocol.Update))
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	x := old[n-1]
	*pq = old[:n-1]
	return x
}

type PostPriorityQueue struct {
	pq PriorityQueue
}

func NewPostPriorityQueue() *PostPriorityQueue {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	return &PostPriorityQueue{pq}
}

func (q *PostPriorityQueue) Push(p *protocol.Update) {
	heap.Push(&q.pq, p)
}

func (q *PostPriorityQueue) Pop() *protocol.Update {
	if q.pq.Len() == 0 {
		return nil
	}
	p := heap.Pop(&q.pq)
	return p.(*protocol.Update)
}

// TODO use heap.Fix to reduce the time complexity
func (q *PostPriorityQueue) Update(p *protocol.Update) {
	back := make([]*protocol.Update, 0)
	for q.Len() > 0 {
		t := q.Pop()
		if t.TaskId != p.TaskId {
			back = append(back, t)
		}
	}

	for _, t := range back {
		q.Push(t)
	}
	q.Push(p)
}

func (q *PostPriorityQueue) Len() int {
	return q.pq.Len()
}

func (q *PostPriorityQueue) GetAll() []*protocol.Update {
	return q.pq
}
