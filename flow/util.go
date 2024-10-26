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

func orDone[T any](done <-chan struct{}, c <-chan T) <-chan T {
	valStream := make(chan T)
	go func() {
		defer close(valStream)
		for {
			select {
			case <-done:
				return
			case val, ok := <-c:
				if !ok {
					return
				}
				// for graceful shutdown, we don't check done channel here
				// because we want to send all values from the channel
				valStream <- val
			}
		}
	}()
	return valStream
}
