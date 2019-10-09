package goduck

import "context"

type RawMessage interface {
	Bytes() []byte
}

type Queue interface {
	Poll(ctx context.Context) (RawMessage, error)
	Done(ctx context.Context, msg RawMessage) error
	Failed(ctx context.Context, msg RawMessage) error
	Close() error
}

type Stream interface {
	Poll(ctx context.Context) (RawMessage, error)
	Done(ctx context.Context) error
	Close() error
}

type Processor interface {
	Process(ctx context.Context, message []byte) error
}
