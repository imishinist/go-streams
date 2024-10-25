package flow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	ext "github.com/imishinist/go-streams/extension"
	"github.com/imishinist/go-streams/flow"
)

func TestBatch(t *testing.T) {
	t.Run("batch", func(t *testing.T) {
		in := make(chan any)
		out := make(chan any)

		source := ext.NewChanSource(in)
		batch := flow.NewBatch[int]("test", 4, 40*time.Millisecond)
		sink := ext.NewChanSink(out)
		assert.NotEqual(t, source, nil)

		inputs := []int{1, 2, 3, 4, 5, 6, 7}
		go func() {
			for _, e := range inputs {
				ingestDeferred(in, e, 5*time.Millisecond)
			}
		}()
		go ingestDeferred(in, 8, 90*time.Millisecond)
		go closeDeferred(in, 100*time.Millisecond)

		go func() {
			source.Via(batch).To(sink)
		}()

		outputs := readSlice[[]int](sink.Out)
		expects := [][]int{{1, 2, 3, 4}, {5, 6, 7}, {8}}
		assert.Equal(t, expects, outputs)
	})

	t.Run("batch with 0 size", func(t *testing.T) {
		in := make(chan any)
		out := make(chan any)

		source := ext.NewChanSource(in)
		batch := flow.NewBatch[int]("test", 0, 40*time.Millisecond)
		sink := ext.NewChanSink(out)
		assert.NotEqual(t, source, nil)

		inputs := []int{1, 2, 3, 4, 5, 6, 7}
		go func() {
			for _, e := range inputs {
				ingestDeferred(in, e, 5*time.Millisecond)
			}
		}()
		go ingestDeferred(in, 8, 90*time.Millisecond)
		go closeDeferred(in, 100*time.Millisecond)

		go func() {
			source.Via(batch).To(sink)
		}()

		outputs := readSlice[[]int](sink.Out)
		expects := [][]int{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}}
		assert.Equal(t, expects, outputs)
	})
}
