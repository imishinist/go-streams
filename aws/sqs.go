package aws

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/imishinist/go-streams"
	"github.com/imishinist/go-streams/flow"
	ssync "github.com/imishinist/go-streams/sync"
)

type QueueMessage[T any] struct {
	ReceiptHandle *string
	Body          *T
}

type SQSSourceConfig[T any] struct {
	QueueURL string

	MaxNumberOfMessages int
	WaitTimeSeconds     int

	Parallelism uint

	BodyHandler func(*string) (*T, error)
}

type SQSSource[T any] struct {
	client   *sqs.Client
	config   *SQSSourceConfig[T]
	reloaded chan struct{}

	out chan any
}

var _ streams.Source = (*SQSSource[string])(nil)

func NewSQSSource[T any](ctx context.Context, client *sqs.Client, config *SQSSourceConfig[T]) *SQSSource[T] {
	sqsSource := &SQSSource[T]{
		client:   client,
		config:   config,
		reloaded: make(chan struct{}),
		out:      make(chan any),
	}
	go sqsSource.receive(ctx)
	return sqsSource
}

func (s *SQSSource[T]) receive(ctx context.Context) {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		close(s.out)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := ssync.NewDynamicSemaphore(s.config.Parallelism)
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.reloaded:
			sem.Set(s.config.Parallelism)
		default:
		}

		sem.Acquire()
		wg.Add(1)
		go func() {
			defer sem.Release()
			defer wg.Done()

			result, err := s.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            &s.config.QueueURL,
				MaxNumberOfMessages: int32(s.config.MaxNumberOfMessages),
				WaitTimeSeconds:     int32(s.config.WaitTimeSeconds),
			})
			if err != nil {
				cancel()
				return
			}

			for _, message := range result.Messages {
				var receiptHandle *string
				body, err := s.config.BodyHandler(message.Body)
				if err == nil {
					receiptHandle = message.ReceiptHandle
				}
				m := QueueMessage[T]{
					ReceiptHandle: receiptHandle,
					Body:          body,
				}
				select {
				case <-s.reloaded:
					sem.Set(s.config.Parallelism)
					s.out <- m
				case s.out <- m:
				}
			}
		}()
	}
}

func (s *SQSSource[T]) Out() <-chan any {
	return s.out
}

func (s *SQSSource[T]) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(s, operator)
	return operator
}

func (s *SQSSource[T]) ReloadConfig(config *SQSSourceConfig[T]) {
	s.config = config
	go func() {
		s.reloaded <- struct{}{}
	}()
}
