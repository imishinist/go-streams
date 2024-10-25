package extension

import (
	"github.com/imishinist/go-streams"
	"github.com/imishinist/go-streams/flow"
)

type ChanSource struct {
	in chan any
}

var _ streams.Source = (*ChanSource)(nil)

func NewChanSource(in chan any) *ChanSource {
	return &ChanSource{in}
}

func (cs *ChanSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(cs, operator)
	return operator
}

func (cs *ChanSource) Out() <-chan any {
	return cs.in
}

type ChanSink struct {
	Out chan any
}

var _ streams.Sink = (*ChanSink)(nil)

func NewChanSink(out chan any) *ChanSink {
	return &ChanSink{out}
}

func (cs *ChanSink) In() chan<- any {
	return cs.Out
}
