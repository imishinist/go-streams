package flow

import (
	"sync"

	"github.com/imishinist/go-streams"
	ssync "github.com/imishinist/go-streams/sync"
)

type MapFunction[T, R any] func(T) R

type Map[T, R any] struct {
	name string

	mapFunction MapFunction[T, R]
	in          chan any
	out         chan any
	parallelism uint

	reloaded chan struct{}
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
		reloaded:    make(chan struct{}),
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
	sem := ssync.NewDynamicSemaphore(m.parallelism)
	defer close(m.out)
	defer close(m.reloaded)

	parallelismGauge.WithLabelValues(m.name, "map").Set(float64(m.parallelism))
	go func() {
		for {
			select {
			case _, ok := <-m.reloaded:
				if !ok {
					return
				}
				sem.Set(m.parallelism)
				parallelismGauge.WithLabelValues(m.name, "map").Set(float64(m.parallelism))
			}
		}
	}()

	wg := new(sync.WaitGroup)
	for elem := range m.in {
		sem.Acquire()
		wg.Add(1)
		workersGauge.WithLabelValues(m.name, "map").Add(1)
		go func(element T) {
			defer func() {
				workersGauge.WithLabelValues(m.name, "map").Sub(1)
				wg.Done()
				sem.Release()
			}()

			m.out <- m.mapFunction(element)
		}(elem.(T))
	}

	wg.Wait()
}

func (m *Map[T, R]) SetParallelism(parallelism uint) {
	m.parallelism = parallelism
	go func() {
		m.reloaded <- struct{}{}
	}()
}
