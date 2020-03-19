package batchstreamengine

import (
	"context"
	"io"
	"time"

	"github.com/arquivei/goduck"
)

type BatchStreamEngine struct {
	streams        []goduck.Stream
	nWorkers       int
	maxBatchSize   int
	maxTimeout     time.Duration
	batchProcessor goduck.BatchProcessor
	done           chan struct{}
}

func New(
	processor goduck.BatchProcessor,
	maxBatchSize int,
	maxBatchTimeout time.Duration,
	streams []goduck.Stream,
) *BatchStreamEngine {
	engine := &BatchStreamEngine{
		streams:        streams,
		nWorkers:       len(streams),
		batchProcessor: processor,
		maxBatchSize:   maxBatchSize,
		maxTimeout:     maxBatchTimeout,
		done:           make(chan struct{}),
	}
	return engine
}

func (e *BatchStreamEngine) Run(ctx context.Context) {
	for i := 0; i < e.nWorkers; i++ {
		go e.pollMessages(ctx, e.streams[i])
	}
	for i := 0; i < e.nWorkers; i++ {
		<-e.done
	}
	close(e.done)
}

func (e *BatchStreamEngine) pollMessages(ctx context.Context, stream goduck.Stream) {
	defer func() {
		e.done <- struct{}{}
	}()
	for ctx.Err() == nil {
		msgs, err := e.pollMessagesBatch(ctx, stream)
		if ctx.Err() != nil {
			break
		}

		if len(msgs) > 0 {
			e.handleMessages(ctx, stream, msgs)
		}

		if err != nil {
			break
		}
	}
}

func (e *BatchStreamEngine) pollMessagesBatch(ctx context.Context, stream goduck.Stream) ([]goduck.RawMessage, error) {
	msgs := make([]goduck.RawMessage, 0, e.maxBatchSize)
	var cancelFn context.CancelFunc
	if e.maxTimeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, e.maxTimeout)
		defer cancelFn()
	}

	for ctx.Err() == nil && len(msgs) < e.maxBatchSize {
		msg, err := stream.Next(ctx)
		if err == io.EOF {
			return msgs, err
		}
		if err != nil {
			continue
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}
func (e *BatchStreamEngine) handleMessages(ctx context.Context, stream goduck.Stream, msgs []goduck.RawMessage) {
	msgBytes := make([][]byte, len(msgs))
	for i, msg := range msgs {
		msgBytes[i] = msg.Bytes()
	}
	for {
		err := e.batchProcessor.BatchProcess(context.Background(), msgBytes)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return
		}
	}
	stream.Done(ctx)
}
