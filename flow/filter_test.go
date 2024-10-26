package flow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	ext "github.com/imishinist/go-streams/extension"
	"github.com/imishinist/go-streams/flow"
)

func TestFilter(t *testing.T) {
	cases := []struct {
		name        string
		filter      func(int) bool
		inputs      []int
		expects     []int
		parallelism uint
	}{
		{
			name: "filter",
			filter: func(i int) bool {
				return i%2 == 0
			},
			inputs:      []int{1, 2, 3, 4, 5, 6, 7},
			expects:     []int{2, 4, 6},
			parallelism: 1,
		},
		{
			name: "filter changing order",
			filter: func(i int) bool {
				// sleep to reverse order
				time.Sleep(time.Duration((10-i)*50) * time.Millisecond)
				return i%2 == 0
			},
			inputs:      []int{1, 2, 3, 4, 5, 6, 7},
			expects:     []int{6, 4, 2},
			parallelism: 10,
		},
	}
	for _, cc := range cases {
		t.Run(cc.name, func(t *testing.T) {
			in := make(chan any, 5)
			out := make(chan any, 5)

			source := ext.NewChanSource(in)
			sink := ext.NewChanSink(out)
			filter := flow.NewFilter[int](cc.name, cc.filter, cc.parallelism)

			go func() {
				defer close(in)
				ingestSlice(in, cc.inputs)
			}()

			source.Via(filter).To(sink)

			outputs := readSlice[int](out)
			assert.Equal(t, cc.expects, outputs)

			labels := map[string]string{"name": cc.name, "type": "filter"}
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
}
