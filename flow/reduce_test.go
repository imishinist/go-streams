package flow_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	ext "github.com/imishinist/go-streams/extension"
	"github.com/imishinist/go-streams/flow"
)

func TestReduce(t *testing.T) {
	t.Run("reduce", func(t *testing.T) {
		in := make(chan any, 10)
		out := make(chan any, 10)

		name := "reduce"
		source := ext.NewChanSource(in)
		reducer := flow.NewReduce[int](name, func(a int, b int) int { return a + b })
		sink := ext.NewChanSink(out)
		go func() {
			source.Via(reducer).To(sink)
		}()

		inputs := []int{1, 2, 3, 4, 5}
		ingestSlice(in, inputs)
		close(in)

		outputs := readSlice[int](out)
		expects := []int{15}
		assert.Equal(t, expects, outputs)

		labels := map[string]string{"name": name, "type": "reduce"}
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
