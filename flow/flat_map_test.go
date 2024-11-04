package flow_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	ext "github.com/imishinist/go-streams/extension"
	"github.com/imishinist/go-streams/flow"
)

func TestFlatMap(t *testing.T) {
	t.Run("flat_map", func(t *testing.T) {
		in := make(chan any, 10)
		out := make(chan any, 10)

		name := "flat_map"
		source := ext.NewChanSource(in)
		mapper := flow.NewFlatMap[int, int](name, func(e int) []int {
			return []int{e * 2, -(e * 2)}
		}, 1)
		sink := ext.NewChanSink(out)
		go func() {
			source.Via(mapper).To(sink)
		}()

		inputs := []int{1, 2, 3, 4, 5}
		ingestSlice(in, inputs)
		close(in)

		outputs := readSlice[int](out)
		expects := []int{2, -2, 4, -4, 6, -6, 8, -8, 10, -10}
		assert.Equal(t, expects, outputs)

		labels := map[string]string{"name": name, "type": "flat_map"}
		gauge, err := flow.ParallelismGauge.GetMetricWith(labels)
		assert.NoError(t, err)
		metrics := readMetrics(t, gauge)
		assert.Len(t, metrics, 1)
		assert.Equal(t, 0, int(metrics[0].value.Gauge.GetValue()))

		gauge, err = flow.WorkersGauge.GetMetricWith(labels)
		assert.NoError(t, err)
		metrics = readMetrics(t, gauge)
		assert.Len(t, metrics, 1)
		assert.Equal(t, 0, int(metrics[0].value.Gauge.GetValue()))
	})
}
