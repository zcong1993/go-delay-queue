package queue

import "container/heap"

// An Item is something we manage in a priority queue.
type Item struct {
	value    interface{} // The value of the item; arbitrary.
	priority int64       // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

func NewItem(value interface{}, priority int64) *Item {
	return &Item{
		value:    value,
		priority: priority,
	}
}

func (it *Item) Priority() int64 {
	return it.priority
}

func (it *Item) Value() interface{} {
	return it.value
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].priority < pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, value string, priority int64) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}

type Pq struct {
	pq PriorityQueue
}

func NewPq() *Pq {
	return &Pq{pq: make(PriorityQueue, 0)}
}

func (pq *Pq) Push(item *Item) {
	heap.Push(&pq.pq, item)
}

func (pq *Pq) Pop() *Item {
	return heap.Pop(&pq.pq).(*Item)
}

func (pq *Pq) Peek() *Item {
	if len(pq.pq) == 0 {
		return nil
	}
	return pq.pq[0]
}
