package sync

import "sync"

// DynamicSemaphore is a semaphore that can change its capacity at runtime.
type DynamicSemaphore struct {
	capacity uint
	count    uint

	mu   sync.RWMutex
	cond *sync.Cond
}

// NewDynamicSemaphore creates a new DynamicSemaphore with the specified initial size.
func NewDynamicSemaphore(initialCapacity uint) *DynamicSemaphore {
	ds := &DynamicSemaphore{
		capacity: initialCapacity,
	}
	ds.cond = sync.NewCond(&ds.mu)
	return ds
}

// Acquire tries to acquire a semaphore slot, blocking until one becomes available.
func (ds *DynamicSemaphore) Acquire() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	for ds.count >= ds.capacity {
		ds.cond.Wait()
	}
	ds.count++
}

// Release releases a semaphore slot, signaling any waiting goroutines.
func (ds *DynamicSemaphore) Release() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.count > 0 {
		ds.count--
		ds.cond.Signal()
	}
}

// Set sets the maximum size of the semaphore.
func (ds *DynamicSemaphore) Set(capacity uint) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	ds.capacity = capacity
	ds.cond.Broadcast()
}

func (ds *DynamicSemaphore) Capacity() uint {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	return ds.capacity
}

func (ds *DynamicSemaphore) Count() uint {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	return ds.count
}
