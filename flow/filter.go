package flow

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/imishinist/go-streams"
)

type FilterPredicate[T any] func(T) bool

type Filter[T any] struct {
	name string

	filterPredicate FilterPredicate[T]
	in              chan any
	out             chan any
	parallelism     uint

	// changed is used to signal that the parallelism has changed.
	changed chan uint
	// finished is used to signal that the stream has finished.
	finished atomic.Bool
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
		changed:         make(chan uint),
		finished:        atomic.Bool{},
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
	// channels for terminating workers individually
	quit := make(chan struct{})
	defer func() {
		close(quit)
		close(f.changed)
		close(f.out)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := new(sync.WaitGroup)
	parallelism := f.parallelism
	for i := 0; i < int(parallelism); i++ {
		wg.Add(1)
		go f.work(wg, quit)
	}

	go func() {
		// reload configurations sequentially
		for newParallelism := range f.changed {
			if newParallelism > parallelism {
				for i := parallelism; i < newParallelism; i++ {
					wg.Add(1)
					go f.work(wg, quit)
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
	f.finished.Store(true)
}

func (f *Filter[T]) work(wg *sync.WaitGroup, quit <-chan struct{}) {
	defer func() {
		workersGauge.WithLabelValues(f.name, "filter").Sub(1)
		wg.Done()
	}()

	workersGauge.WithLabelValues(f.name, "filter").Add(1)
	for v := range orDone(quit, f.in) {
		elem := v.(T)
		if f.filterPredicate(elem) {
			f.out <- elem
		}
	}
}

func (f *Filter[T]) SetParallelism(parallelism uint) {
	if parallelism == 0 {
		parallelism = 1
	}
	if f.finished.Load() || f.parallelism == parallelism {
		return
	}

	f.parallelism = parallelism
	parallelismGauge.WithLabelValues(f.name, "filter").Set(float64(parallelism))
	go func() {
		f.changed <- parallelism
	}()
}
