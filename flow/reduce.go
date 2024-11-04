package flow

import "github.com/imishinist/go-streams"

type ReduceFunction[T any] func(T, T) T

type Reduce[T any] struct {
	name string

	reduceFunc ReduceFunction[T]
	in         chan any
	out        chan any

	lastReduced any
}

var _ streams.Flow = (*Reduce[any])(nil)

func NewReduce[T any](name string, reduceFunction ReduceFunction[T]) *Reduce[T] {
	reduce := &Reduce[T]{
		name:       name,
		reduceFunc: reduceFunction,
		in:         make(chan any),
		out:        make(chan any),
	}
	workersGauge.WithLabelValues(name, "reduce").Set(0)
	parallelismGauge.WithLabelValues(name, "reduce").Set(1)
	go reduce.doStream()

	return reduce
}

func (r *Reduce[T]) In() chan<- any {
	return r.in
}

func (r *Reduce[T]) Out() <-chan any {
	return r.out
}

func (r *Reduce[T]) Via(flow streams.Flow) streams.Flow {
	go r.transmit(flow)
	return flow
}

func (r *Reduce[T]) To(sink streams.Sink) {
	r.transmit(sink)
}

func (r *Reduce[T]) transmit(in streams.Input) {
	defer func() {
		close(in.In())
		parallelismGauge.WithLabelValues(r.name, "reduce").Set(0)
	}()
	for elem := range r.Out() {
		in.In() <- elem
	}
}

func (r *Reduce[T]) doStream() {
	defer func() {
		workersGauge.WithLabelValues(r.name, "reduce").Sub(1)
		close(r.out)
	}()
	workersGauge.WithLabelValues(r.name, "reduce").Add(1)
	for elem := range r.in {
		if r.lastReduced == nil {
			r.lastReduced = elem.(T)
		} else {
			r.lastReduced = r.reduceFunc(r.lastReduced.(T), elem.(T))
		}
	}
	r.out <- r.lastReduced
}
