package flow

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/imishinist/go-streams"
)

type MapFunction[T, R any] func(T) R

type Map[T, R any] struct {
	name string

	mapFunction MapFunction[T, R]
	in          chan any
	out         chan any
	parallelism uint

	// changed is used to signal that the parallelism has changed.
	changed chan uint
	// finished is used to signal that the stream has finished.
	finished atomic.Bool
}

var _ streams.Flow = (*Map[any, any])(nil)

func NewMap[T, R any](name string, mapFunction MapFunction[T, R], parallelism uint) *Map[T, R] {
	if parallelism == 0 {
		parallelism = 1
	}
	mapFlow := &Map[T, R]{
		name:        name,
		mapFunction: mapFunction,
		in:          make(chan any),
		out:         make(chan any),
		parallelism: parallelism,
		changed:     make(chan uint),
		finished:    atomic.Bool{},
	}
	workersGauge.WithLabelValues(name, "map").Set(0)
	parallelismGauge.WithLabelValues(name, "map").Set(float64(parallelism))
	go mapFlow.doStream()

	return mapFlow
}

func (m *Map[T, R]) Via(flow streams.Flow) streams.Flow {
	go m.transmit(flow)
	return flow
}

func (m *Map[T, R]) To(sink streams.Sink) {
	m.transmit(sink)
}

func (m *Map[T, R]) Out() <-chan any {
	return m.out
}

func (m *Map[T, R]) In() chan<- any {
	return m.in
}

func (m *Map[T, R]) transmit(inlet streams.Input) {
	defer func() {
		close(inlet.In())
		parallelismGauge.WithLabelValues(m.name, "map").Set(0)
	}()
	for element := range m.Out() {
		inlet.In() <- element
	}
}

func (m *Map[T, R]) doStream() {
	// channels for terminating workers individually
	quit := make(chan struct{})
	defer func() {
		close(quit)
		close(m.changed)
		close(m.out)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := new(sync.WaitGroup)
	parallelism := m.parallelism
	for i := 0; i < int(parallelism); i++ {
		wg.Add(1)
		go m.work(wg, quit)
	}

	go func() {
		// reload configurations sequentially
		for newParallelism := range m.changed {
			if newParallelism > parallelism {
				for i := parallelism; i < newParallelism; i++ {
					wg.Add(1)
					go m.work(wg, quit)
				}
			} else {
				for i := parallelism; i > newParallelism; i-- {
					select {
					case quit <- struct{}{}:
					case <-ctx.Done():
						return
					}
				}
			}
			parallelism = newParallelism
		}
	}()

	wg.Wait()
	m.finished.Store(true)
}

func (m *Map[T, R]) work(wg *sync.WaitGroup, quit <-chan struct{}) {
	defer func() {
		workersGauge.WithLabelValues(m.name, "map").Sub(1)
		wg.Done()
	}()

	workersGauge.WithLabelValues(m.name, "map").Add(1)
	for v := range orDone(quit, m.in) {
		m.out <- m.mapFunction(v.(T))
	}
}

func (m *Map[T, R]) SetParallelism(parallelism uint) {
	if parallelism == 0 {
		parallelism = 1
	}
	if m.finished.Load() || m.parallelism == parallelism {
		return
	}

	m.parallelism = parallelism
	parallelismGauge.WithLabelValues(m.name, "map").Set(float64(parallelism))
	go func() {
		m.changed <- parallelism
	}()
}
