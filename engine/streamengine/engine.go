package streamengine

import (
	"context"
	"io"
	"sync"

	"github.com/arquivei/goduck"

	"github.com/arquivei/foundationkit/errors"
)

type StreamEngine struct {
	streams   []goduck.Stream
	nWorkers  int
	processor goduck.Processor
	workersWG *sync.WaitGroup

	cancelFn       func()
	processorError error
}

func New(processor goduck.Processor, streams []goduck.Stream) *StreamEngine {
	engine := &StreamEngine{
		streams:        streams,
		nWorkers:       len(streams),
		processor:      processor,
		workersWG:      &sync.WaitGroup{},
		cancelFn:       nil,
		processorError: nil,
	}
	return engine
}

func (e *StreamEngine) Run(ctx context.Context) error {
	ctx, e.cancelFn = context.WithCancel(ctx)

	e.workersWG.Add(e.nWorkers)
	for i := 0; i < e.nWorkers; i++ {
		go e.pollMessages(ctx, e.streams[i])
	}
	e.workersWG.Wait()
	return e.processorError
}

func (e *StreamEngine) pollMessages(ctx context.Context, stream goduck.Stream) {
	defer e.workersWG.Done()
	for ctx.Err() == nil {
		msg, err := stream.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		e.handleMessage(ctx, stream, msg)
	}
}
func (e *StreamEngine) handleMessage(ctx context.Context, stream goduck.Stream, msg goduck.RawMessage) {
	for {
		err := e.processor.Process(context.Background(), msg.Bytes())
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

func (e *StreamEngine) selfClose(err error) {
	e.cancelFn()
	e.processorError = err
}
