package semaphore

import (
	"sync"
)

type Semaphore struct {
	val  int
	cond *sync.Cond
}

func New(val int) *Semaphore {
	return &Semaphore{val, sync.NewCond(&sync.Mutex{})}
}

func (s *Semaphore) Acquire() {
	s.cond.L.Lock()
	for !(s.val > 0) {
		s.cond.Wait()
	}
	s.val--
	s.cond.L.Unlock()
}

func (s *Semaphore) Release() {
	s.cond.L.Lock()
	s.val++
	s.cond.Signal()
	s.cond.L.Unlock()
}
