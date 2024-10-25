package flow_test

import "time"

func ingestDeferred[T any](in chan any, item T, wait time.Duration) {
	time.Sleep(wait)
	in <- item
}

func closeDeferred(in chan any, wait time.Duration) {
	time.Sleep(wait)
	close(in)
}

func readSlice[T any](ch <-chan any) []T {
	var result []T
	for e := range ch {
		result = append(result, e.(T))
	}
	return result
}
