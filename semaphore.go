package nxsugar

type semaphore struct {
	ch chan struct{}
}

func newSemaphore(n int) *semaphore {
	return &semaphore{
		ch: make(chan struct{}, n),
	}
}

func (s *semaphore) Acquire() {
	s.ch <- struct{}{}
}
func (s *semaphore) Release() {
	<-s.ch
}
func (s *semaphore) Used() int {
	return len(s.ch)
}
func (s *semaphore) Free() int {
	return len(s.ch) - cap(s.ch)
}
func (s *semaphore) Cap() int {
	return cap(s.ch)
}
