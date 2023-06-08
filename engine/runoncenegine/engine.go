package runoncenegine

import (
	"context"
	"io"
	"time"

	"github.com/arquivei/goduck"
	"github.com/arquivei/goduck/gokithelper"

	"github.com/arquivei/foundationkit/errors"
	"github.com/go-kit/kit/endpoint"
)

// RunOnceEngine is an engine that processes a batch of messages only once and
// then shuts down
type RunOnceEngine struct {
	stream         goduck.Stream
	maxBatchSize   int
	maxTimeout     time.Duration
	batchProcessor goduck.BatchProcessor

	cancelFn       func()
	processorError error
}

// NewFromEndpoint creates a BatchProcessor from a go-kit endpoint
func NewFromEndpoint(
	e endpoint.Endpoint,
	decoder goduck.EndpointBatchDecoder,
	maxBatchSize int,
	maxTimeout time.Duration,
	stream goduck.Stream,
) *RunOnceEngine {
	return New(
		gokithelper.MustNewEndpointBatchProcessor(e, decoder),
		maxBatchSize,
		maxTimeout,
		stream,
	)
}

// New creates a new RunOnceEngine.
func New(
	processor goduck.BatchProcessor,
	maxBatchSize int,
	maxTimeout time.Duration,
	stream goduck.Stream,
) *RunOnceEngine {
	engine := &RunOnceEngine{
		stream:         stream,
		batchProcessor: processor,
		maxBatchSize:   maxBatchSize,
		maxTimeout:     maxTimeout,
		processorError: nil,
	}
	return engine
}

// Run processes the messages and then closes
func (e *RunOnceEngine) Run(ctx context.Context) error {
	e.pollMessages(ctx, e.stream)
	return e.processorError
}

func (e *RunOnceEngine) pollMessages(ctx context.Context, stream goduck.Stream) {
	msgs, _ := e.pollMessagesBatch(ctx, stream)

	if len(msgs) > 0 {
		e.handleMessages(ctx, stream, msgs)
	}
}

func (e *RunOnceEngine) pollMessagesBatch(ctx context.Context, stream goduck.Stream) ([]goduck.RawMessage, error) {
	msgs := []goduck.RawMessage{}
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

func (e *RunOnceEngine) handleMessages(ctx context.Context, stream goduck.Stream, msgs []goduck.RawMessage) {
	msgBytes := make([][]byte, len(msgs))
	for i, msg := range msgs {
		msgBytes[i] = msg.Bytes()
	}

	err := e.batchProcessor.BatchProcess(context.Background(), msgBytes)
	if err != nil && errors.GetSeverity(err) == errors.SeverityFatal {
		e.selfClose(err)
		return
	}
	if ctx.Err() != nil {
		return
	}
	stream.Done(ctx)
}

func (e *RunOnceEngine) selfClose(err error) {
	e.processorError = err
}
