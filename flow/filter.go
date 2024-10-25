package flow

import (
	"sync"

	"github.com/imishinist/go-streams"
	ssync "github.com/imishinist/go-streams/sync"
)

type FilterPredicate[T any] func(T) bool

type Filter[T any] struct {
	name string

	filterPredicate FilterPredicate[T]
	in              chan any
	out             chan any
	parallelism     uint

	reloaded chan struct{}
}

var _ streams.Flow = (*Filter[any])(nil)

func NewFilter[T any](name string, filterPredicate FilterPredicate[T], parallelism uint) *Filter[T] {
	if parallelism == 0 {
		parallelism = 1
	}

	filter := &Filter[T]{
		name:            name,
		filterPredicate: filterPredicate,
		in:              make(chan any),
		out:             make(chan any),
		parallelism:     parallelism,
		reloaded:        make(chan struct{}),
	}
	workersGauge.WithLabelValues(name, "filter").Set(0)
	parallelismGauge.WithLabelValues(name, "filter").Set(float64(parallelism))
	go filter.doStream()

	return filter
}

func (f *Filter[T]) Via(flow streams.Flow) streams.Flow {
	go f.transmit(flow)
	return flow
}

func (f *Filter[T]) To(sink streams.Sink) {
	f.transmit(sink)
}

func (f *Filter[T]) Out() <-chan any {
	return f.out
}

func (f *Filter[T]) In() chan<- any {
	return f.in
}

func (f *Filter[T]) transmit(inlet streams.Input) {
	defer func() {
		close(inlet.In())
		parallelismGauge.WithLabelValues(f.name, "filter").Set(0)
	}()
	for element := range f.Out() {
		inlet.In() <- element
	}
}

// doStream discards items that don't match the filter predicate.
func (f *Filter[T]) doStream() {
	sem := ssync.NewDynamicSemaphore(f.parallelism)
	defer close(f.out)
	defer close(f.reloaded)

	parallelismGauge.WithLabelValues(f.name, "filter").Set(float64(f.parallelism))
	go func() {
		for {
			select {
			case _, ok := <-f.reloaded:
				if !ok {
					return
				}
				sem.Set(f.parallelism)
				parallelismGauge.WithLabelValues(f.name, "filter").Set(float64(f.parallelism))
			}
		}
	}()

	wg := new(sync.WaitGroup)
	for elem := range f.in {
		sem.Acquire()
		wg.Add(1)
		workersGauge.WithLabelValues(f.name, "filter").Add(1)
		go func(element T) {
			defer func() {
				workersGauge.WithLabelValues(f.name, "filter").Sub(1)
				wg.Done()
				sem.Release()
			}()

			if f.filterPredicate(element) {
				f.out <- element
			}
		}(elem.(T))
	}
	wg.Wait()
}

func (f *Filter[T]) SetParallelism(parallelism uint) {
	f.parallelism = parallelism
	go func() {
		f.reloaded <- struct{}{}
	}()
}
