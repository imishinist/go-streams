package flow

import (
	"time"

	"github.com/imishinist/go-streams"
)

type Batch[T any] struct {
	name string

	maxBatchSize uint
	timeInterval time.Duration
	in           chan any
	out          chan any

	reloaded chan struct{}
}

var _ streams.Flow = (*Batch[any])(nil)

func NewBatch[T any](name string, maxBatchSize uint, timeInterval time.Duration) *Batch[T] {
	if maxBatchSize == 0 {
		maxBatchSize = 1
	}

	batchFlow := &Batch[T]{
		name:         name,
		maxBatchSize: maxBatchSize,
		timeInterval: timeInterval,
		in:           make(chan any),
		out:          make(chan any),
		reloaded:     make(chan struct{}),
	}
	workersGauge.WithLabelValues(name, "batch").Set(0)
	parallelismGauge.WithLabelValues(name, "batch").Set(float64(1))
	go batchFlow.batchStream()

	return batchFlow
}

func (b *Batch[T]) Via(flow streams.Flow) streams.Flow {
	go b.transmit(flow)
	return flow
}

func (b *Batch[T]) To(sink streams.Sink) {
	b.transmit(sink)
}

func (b *Batch[T]) Out() <-chan any {
	return b.out
}

func (b *Batch[T]) In() chan<- any {
	return b.in
}

func (b *Batch[T]) transmit(inlet streams.Input) {
	defer func() {
		close(inlet.In())
		parallelismGauge.WithLabelValues(b.name, "batch").Set(0)
	}()

	for batch := range b.out {
		inlet.In() <- batch
	}
}

func (b *Batch[T]) batchStream() {
	ticker := time.NewTicker(b.timeInterval)
	defer func() {
		close(b.out)
		ticker.Stop()
		workersGauge.WithLabelValues(b.name, "batch").Sub(1)
	}()
	workersGauge.WithLabelValues(b.name, "batch").Set(1)

	// If you want to reload the configuration, make the local variables
	// reflect the values only after receiving the value from the "reloaded" channel.
	maxBatchSize := b.maxBatchSize
	timeInterval := b.timeInterval
	batch := make([]T, 0, maxBatchSize)
	for {
		select {
		case <-b.reloaded:
			maxBatchSize = b.maxBatchSize
			timeInterval = b.timeInterval
		case element, ok := <-b.in:
			if !ok {
				if len(batch) > 0 {
					b.out <- batch
				}
				return
			}

			batch = append(batch, element.(T))
			if len(batch) >= int(maxBatchSize) {
				b.out <- batch
				batch = make([]T, 0, maxBatchSize)
			}
			ticker.Reset(timeInterval)
		case <-ticker.C:
			if len(batch) > 0 {
				b.out <- batch
				batch = make([]T, 0, b.maxBatchSize)
			}
		}
	}
}

func (b *Batch[T]) SetConfig(maxBatchSize uint, timeInterval time.Duration) {
	b.maxBatchSize = maxBatchSize
	b.timeInterval = timeInterval
	go func() {
		b.reloaded <- struct{}{}
	}()
}
