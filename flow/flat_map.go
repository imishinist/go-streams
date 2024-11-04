package flow

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/imishinist/go-streams"
)

type FlatMapFunction[T, R any] func(T) []R

type FlatMap[T, R any] struct {
	name string

	flatMapFunction FlatMapFunction[T, R]

	in          chan any
	out         chan any
	parallelism uint

	// changed is used to signal that the parallelism has changed.
	changed chan uint
	// finished is used to signal that the stream has finished.
	finished atomic.Bool
}

var _ streams.Flow = (*FlatMap[any, any])(nil)

func NewFlatMap[T, R any](name string, flatMapFunction FlatMapFunction[T, R], parallelism uint) *FlatMap[T, R] {
	if parallelism == 0 {
		parallelism = 1
	}
	flatMap := &FlatMap[T, R]{
		name:            name,
		flatMapFunction: flatMapFunction,
		in:              make(chan any),
		out:             make(chan any),
		parallelism:     parallelism,
		changed:         make(chan uint),
		finished:        atomic.Bool{},
	}
	workersGauge.WithLabelValues(name, "flat_map").Set(0)
	parallelismGauge.WithLabelValues(name, "flat_map").Set(float64(parallelism))
	go flatMap.doStream()
	return flatMap
}

func (fm *FlatMap[T, R]) Via(flow streams.Flow) streams.Flow {
	go fm.transmit(flow)
	return flow
}

func (fm *FlatMap[T, R]) To(sink streams.Sink) {
	fm.transmit(sink)
}

func (fm *FlatMap[T, R]) Out() <-chan any {
	return fm.out
}

func (fm *FlatMap[T, R]) In() chan<- any {
	return fm.in
}

func (fm *FlatMap[T, R]) transmit(dst streams.Input) {
	defer func() {
		close(dst.In())
		parallelismGauge.WithLabelValues(fm.name, "flat_map").Set(0)
	}()

	for elem := range fm.Out() {
		dst.In() <- elem
	}
}

func (fm *FlatMap[T, R]) doStream() {
	// channels for terminating workers individually
	quit := make(chan struct{})
	defer func() {
		close(quit)
		close(fm.changed)
		close(fm.out)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := new(sync.WaitGroup)
	parallelism := fm.parallelism
	for i := 0; i < int(parallelism); i++ {
		wg.Add(1)
		go fm.work(wg, quit)
	}

	go func() {
		// reload configurations sequentially
		for newParallelism := range fm.changed {
			if newParallelism > parallelism {
				for i := parallelism; i < newParallelism; i++ {
					wg.Add(1)
					go fm.work(wg, quit)
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
	fm.finished.Store(true)
}

func (fm *FlatMap[T, R]) work(wg *sync.WaitGroup, quit <-chan struct{}) {
	defer func() {
		workersGauge.WithLabelValues(fm.name, "flat_map").Sub(1)
		wg.Done()
	}()

	workersGauge.WithLabelValues(fm.name, "flat_map").Add(1)
	for v := range orDone(quit, fm.in) {
		result := fm.flatMapFunction(v.(T))
		for _, r := range result {
			fm.out <- r
		}
	}
}
