package kafkaconfluent

import (
	"context"
	"io"

	"github.com/arquivei/foundationkit/errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
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

func newRequestController(done chan struct{}) *requestController {
	return &requestController{
		isPending:          false,
		workerNotification: make(chan struct{}, 1),
		resultChannel:      make(chan *kafka.Message),
		done:               done,
	}
}

// requestJob notifies the worker that a new job must be processed. If the
// worker was already notified, it does nothing.
func (r *requestController) requestJob() error {
	if r.isPending {
		// someone already requested a job, no further action is needed
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

// getResult waits for the worker to complete the job and returns the result.
// 3 things can happen here:
//   - The job result arrives. This puts the controller in the !pending state.
//   - The controller is closed. Returns EOF
//   - The context expired. An error is returned, but getResult can be
//     called again later.
func (r *requestController) getResult(ctx context.Context) (*kafka.Message, error) {
	const op = errors.Op("getResult")

	select {
	case item := <-r.resultChannel:
		r.isPending = false
		return item, nil
	case <-r.done:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, errors.E(op, ctx.Err())
	}
}

// getNextJob waits until the requester sends a new job. If the controller
// closes, returns EOF
func (r *requestController) getNextJob() error {
	select {
	case <-r.workerNotification:
		return nil
	case <-r.done:
		return io.EOF
	}
}

// submitResult waits until the requester receiver gets the result. If the
// controller closes, returns EOF
func (r *requestController) submitResult(item *kafka.Message) error {
	select {
	case r.resultChannel <- item:
		return nil
	case <-r.done:
		return io.EOF
	}
}
