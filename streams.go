package streams

// Input is an interface for a stream input.
type Input interface {
	In() chan<- any
}

// Output is an interface for a stream output.
type Output interface {
	Out() <-chan any
}

// Source is an interface for a stream source.
type Source interface {
	Output
	Via(Flow) Flow
}

// Flow is an interface for a stream flow.
type Flow interface {
	Input
	Output
	Via(Flow) Flow
	To(Sink)
}

// Sink is an interface for a stream sink.
type Sink interface {
	Input
}
