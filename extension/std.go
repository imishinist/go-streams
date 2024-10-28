package extension

import (
	"fmt"

	"github.com/imishinist/go-streams"
)

type StdoutSink struct {
	in chan any
}

var _ streams.Sink = (*StdoutSink)(nil)

func NewStdoutSink() *StdoutSink {
	sink := &StdoutSink{
		in: make(chan any),
	}
	sink.init()
	return sink
}

func (s *StdoutSink) init() {
	go func() {
		for elem := range s.in {
			fmt.Println(elem)
		}
	}()
}

func (s *StdoutSink) In() chan<- any {
	return s.in
}

type IgnoreSink struct {
	in chan any
}

func NewIgnoreSink() *IgnoreSink {
	sink := &IgnoreSink{
		in: make(chan any),
	}
	sink.init()
	return sink
}

func (s *IgnoreSink) init() {
	go func() {
		for {
			_, ok := <-s.in
			if !ok {
				return
			}
		}
	}()
}

func (s *IgnoreSink) In() chan<- any {
	return s.in
}
