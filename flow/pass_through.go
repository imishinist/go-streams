package flow

import "github.com/imishinist/go-streams"

type PassThrough struct {
	name string

	in  chan any
	out chan any
}

var _ streams.Flow = (*PassThrough)(nil)

func NewPassThrough(name string) *PassThrough {
	pass := &PassThrough{
		name: name,
		in:   make(chan any),
		out:  make(chan any),
	}
	go pass.doStream()
	return pass
}

func (p *PassThrough) In() chan<- any {
	return p.in
}

func (p *PassThrough) Out() <-chan any {
	return p.out
}

func (p *PassThrough) Via(flow streams.Flow) streams.Flow {
	go p.transmit(flow)
	return flow
}

func (p *PassThrough) To(sink streams.Sink) {
	p.transmit(sink)
}

func (p *PassThrough) transmit(dst streams.Input) {
	defer close(dst.In())
	for elem := range p.Out() {
		dst.In() <- elem
	}
}

func (p *PassThrough) doStream() {
	defer close(p.out)
	for elem := range p.in {
		p.out <- elem
	}
}
