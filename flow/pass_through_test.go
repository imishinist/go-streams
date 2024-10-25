package flow_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	ext "github.com/imishinist/go-streams/extension"
	"github.com/imishinist/go-streams/flow"
)

func TestPassThrough(t *testing.T) {
	in := make(chan any, 10)
	out := make(chan any, 10)

	source := ext.NewChanSource(in)
	pass := flow.NewPassThrough("pass_through")
	sink := ext.NewChanSink(out)
	go func() {
		source.Via(pass).To(sink)
	}()

	inputs := []int{1, 2, 3, 4, 5}
	ingestSlice(in, inputs)
	close(in)

	outputs := readSlice[int](out)
	assert.Equal(t, inputs, outputs)
}
