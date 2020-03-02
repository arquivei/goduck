package jobpoolengine

import (
	"context"
	"io"

	"github.com/arquivei/goduck"
)

type JobPoolEngine struct {
	queue       goduck.MessagePool
	nextMessage chan goduck.RawMessage
	nWorkers    int
	processor   goduck.Processor
}

func New(queue goduck.MessagePool, processor goduck.Processor, nWorkers int) JobPoolEngine {
	engine := JobPoolEngine{
		queue:       queue,
		nextMessage: make(chan goduck.RawMessage),
		nWorkers:    nWorkers,
		processor:   processor,
	}
	return engine
}

func (e JobPoolEngine) Run(ctx context.Context) {
	for i := 0; i < e.nWorkers; i++ {
		go e.handleMessages(context.Background())
	}
	e.pollMessages(ctx)
}

func (e JobPoolEngine) pollMessages(ctx context.Context) {
	defer close(e.nextMessage)
	for {
		msg, err := e.queue.Next(ctx)
		if err == io.EOF {
			return
		}
		if err != nil {
			continue
		}
		select {
		case e.nextMessage <- msg:
			continue
		case <-ctx.Done():
			return
		}
	}

}

func (e JobPoolEngine) handleMessages(ctx context.Context) {
	for {
		msg, ok := <-e.nextMessage
		if !ok {
			return
		}
		e.handleMessage(ctx, msg)
	}
}

func (e JobPoolEngine) handleMessage(ctx context.Context, msg goduck.RawMessage) {
	err := e.processor.Process(ctx, msg.Bytes())
	if err == nil {
		e.queue.Done(ctx, msg)
	} else {
		e.queue.Failed(ctx, msg)
	}
	// Ack/Nack errors are ignored
}
