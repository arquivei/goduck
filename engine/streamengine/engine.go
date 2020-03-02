package streamengine

import (
	"context"
	"io"

	"github.com/arquivei/goduck"
)

type StreamEngine struct {
	streams   []goduck.Stream
	nWorkers  int
	processor goduck.Processor
	done      chan struct{}
}

func New(processor goduck.Processor, streams []goduck.Stream) StreamEngine {
	engine := StreamEngine{
		streams:   streams,
		nWorkers:  len(streams),
		processor: processor,
		done:      make(chan struct{}),
	}
	return engine
}

func (e StreamEngine) Run(ctx context.Context) {
	for i := 0; i < e.nWorkers; i++ {
		go e.pollMessages(ctx, e.streams[i])
	}
	for i := 0; i < e.nWorkers; i++ {
		<-e.done
	}
	close(e.done)
}

func (e StreamEngine) pollMessages(ctx context.Context, stream goduck.Stream) {
	defer func() {
		e.done <- struct{}{}
	}()
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
func (e StreamEngine) handleMessage(ctx context.Context, stream goduck.Stream, msg goduck.RawMessage) {
	for {
		err := e.processor.Process(context.Background(), msg.Bytes())
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return
		}
	}
	stream.Done(ctx)
}
