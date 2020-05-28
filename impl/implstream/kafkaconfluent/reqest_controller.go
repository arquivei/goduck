package kafkaconfluent

import (
	"context"
	"io"

	"github.com/arquivei/foundationkit/errors"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// requestController implements a job pool model, where the worker is either
// processing or waiting for the next job. The requester can request a job
// multiple times, but if the worker is already processing, nothing will
// happen.
// This is useful because the requester can "give up" waiting for the result
// and come back again later. In that case, the "request" operation must be
// idempotent.
type requestController struct {
	isPending          bool
	workerNotification chan struct{}
	resultChannel      chan *kafka.Message
	done               chan struct{}
}

func newRequestController() *requestController {
	return &requestController{
		isPending:          false,
		workerNotification: make(chan struct{}, 1),
		resultChannel:      make(chan *kafka.Message),
	}
}

func (r *requestController) requestJob() error {
	if r.isPending {
		return nil
	}
	r.isPending = true

	select {
	case r.workerNotification <- struct{}{}:
		return nil
	case <-r.done:
		return io.EOF
	}
}

func (r *requestController) getResult(ctx context.Context) (*kafka.Message, error) {
	const op = errors.Op("getResult")
	select {
	case item, ok := <-r.resultChannel:
		r.isPending = false
		if !ok {
			return nil, io.EOF
		}
		return item, nil
	case <-ctx.Done():
		return nil, errors.E(op, ctx.Err())
	}
}

func (r *requestController) getNextJob() error {
	select {
	case <-r.workerNotification:
		return nil
	case <-r.done:
		return io.EOF
	}
}

func (r *requestController) submitResult(item *kafka.Message) error {
	select {
	case r.resultChannel <- item:
		return nil
	case <-r.done:
		return io.EOF
	}
}

// close makes all future requests return an io.EOF error
func (r *requestController) close() {
	close(r.resultChannel)
}
