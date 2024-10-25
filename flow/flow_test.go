package flow_test

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func ingestSlice[T any](in chan any, sources []T) {
	for _, e := range sources {
		in <- e
	}
}

func ingestDeferred[T any](in chan any, item T, wait time.Duration) {
	time.Sleep(wait)
	in <- item
}

func closeDeferred(in chan any, wait time.Duration) {
	time.Sleep(wait)
	close(in)
}

func readSlice[T any](ch <-chan any) []T {
	var result []T
	for e := range ch {
		result = append(result, e.(T))
	}
	return result
}

type metricValue struct {
	value dto.Metric
}

func readMetrics(t *testing.T, collector prometheus.Collector) []metricValue {
	var result []metricValue
	metrics := make(chan prometheus.Metric)
	go func() {
		defer close(metrics)
		collector.Collect(metrics)
	}()

	for m := range metrics {
		var value dto.Metric
		if err := m.Write(&value); err != nil {
			t.Error(err)
		}
		result = append(result, metricValue{
			value: value,
		})
	}
	return result
}
