package flow

import "github.com/imishinist/go-streams"

// DoStream is a utility function to connect a stream output to a stream input.
// Once the value is read, the value is sent synchronously to the destination channel,
// so if the destination channel is blocked, the value is buffered here
func DoStream(out streams.Output, in streams.Input) {
	go func() {
		defer close(in.In())

		for elem := range out.Out() {
			in.In() <- elem
		}
	}()
}
