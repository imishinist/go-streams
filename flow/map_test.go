package flow_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	ext "github.com/imishinist/go-streams/extension"
	"github.com/imishinist/go-streams/flow"
)

func TestMap(t *testing.T) {
	t.Run("map", func(t *testing.T) {
		in := make(chan any, 10)
		out := make(chan any, 10)

		source := ext.NewChanSource(in)
		mapper := flow.NewMap[int, int]("map", func(e int) int { return e * 2 }, 1)
		sink := ext.NewChanSink(out)
		go func() {
			source.Via(mapper).To(sink)
		}()

		inputs := []int{1, 2, 3, 4, 5}
		ingestSlice(in, inputs)
		close(in)

		outputs := readSlice[int](out)
		expects := []int{2, 4, 6, 8, 10}
		assert.Equal(t, expects, outputs)
	})
}
