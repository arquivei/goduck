package batchstreamengine

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/arquivei/goduck"
	"github.com/arquivei/goduck/middleware/processormiddleware"

	"github.com/arquivei/foundationkit/errors"
	"github.com/go-kit/kit/endpoint"
)

// BatchStreamEngine is an engine that processes a batch of messages from
// a stream, with the order preserved.
type BatchStreamEngine struct {
	streams        []goduck.Stream
	nWorkers       int
	maxBatchSize   int
	maxTimeout     time.Duration
	batchProcessor goduck.BatchProcessor
	workersWG      *sync.WaitGroup

	cancelFn       func()
	processorError error
}

// NewFromEndpoint creates a BatchProcessor from a go-kit endpoint
func NewFromEndpoint(
	processor endpoint.Endpoint,
	maxBatchSize int,
	maxBatchTimeout time.Duration,
	streams []goduck.Stream,
) *BatchStreamEngine {
	return New(
		processormiddleware.WrapEndpointInProcessor(processor),
		maxBatchSize,
		maxBatchTimeout,
		streams,
	)
}

// New creates a new BackStreamEngine.
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
		workersWG:      &sync.WaitGroup{},
		cancelFn:       nil,
		processorError: nil,
	}
	return engine
}

// Run starts processing the messages, until @ctx is closed
func (e *BatchStreamEngine) Run(ctx context.Context) error {
	ctx, e.cancelFn = context.WithCancel(ctx)

	e.workersWG.Add(e.nWorkers)
	for i := 0; i < e.nWorkers; i++ {
		go e.pollMessages(ctx, e.streams[i])
	}
	e.workersWG.Wait()

	return e.processorError
}

func (e *BatchStreamEngine) pollMessages(ctx context.Context, stream goduck.Stream) {
	defer e.workersWG.Done()
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
		if errors.GetSeverity(err) == errors.SeverityFatal {
			e.selfClose(err)
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
	stream.Done(ctx)
}

func (e *BatchStreamEngine) selfClose(err error) {
	e.cancelFn()
	e.processorError = err
}
