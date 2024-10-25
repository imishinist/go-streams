package streams

import "testing"

type input struct{}

func (i *input) In() chan<- any {
	panic("dummy implementation")
}

func (i *input) Change(param any) {
	panic("dummy implementation")
}

type output struct{}

func (o *output) Out() <-chan any {
	panic("dummy implementation")
}

type source struct {
	output
}

func (s *source) Via(f Flow) Flow {
	panic("dummy implementation")
}

func (s *source) Change(param any) {
	panic("dummy implementation")
}

type flow struct {
	input
	output
}

func (f *flow) Via(f2 Flow) Flow {
	panic("dummy implementation")
}

func (f *flow) To(s Sink) {
	panic("dummy implementation")
}

func (f *flow) Change(param any) {
	panic("dummy implementation")
}

func TestStreamInterface(t *testing.T) {
	var _ Source = (*source)(nil)
	var _ Flow = (*flow)(nil)
	var _ Sink = (*input)(nil)
}
