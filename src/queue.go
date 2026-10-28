package src

import "sync"

// Queue is an unbounded FIFO queue; Pop drains remaining items after Close.
type Queue[T any] struct {
	mu     sync.Mutex
	items  []T
	closed bool
	signal chan struct{}
}

func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{signal: make(chan struct{}, 1)}
}

func (q *Queue[T]) notify() {
	select {
	case q.signal <- struct{}{}:
	default:
	}
}

// Push returns false if the queue is already closed.
func (q *Queue[T]) Push(item T) bool {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}
	q.items = append(q.items, item)
	q.mu.Unlock()
	q.notify()
	return true
}

func (q *Queue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (q *Queue[T]) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.notify()
}

// Pop blocks until an item is available; it returns false when the queue is closed and empty.
func (q *Queue[T]) Pop() (T, bool) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			item := q.items[0]
			q.items = q.items[1:]
			q.mu.Unlock()
			return item, true
		}
		if q.closed {
			q.mu.Unlock()
			var zero T
			return zero, false
		}
		q.mu.Unlock()
		<-q.signal
	}
}
