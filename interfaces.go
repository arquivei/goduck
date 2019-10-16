package goduck

import "context"

// RawMessage is the interface that Queue/Stream sources should provide.
// It can contain some internal control variables, such as MessageID, useful
// for marking the message as complete.
type RawMessage interface {
	Bytes() []byte
}

// MessagePool represents a pool of unordered messages, where each one can be
// individually marked as complete.
type MessagePool interface {
	// Next should return the next message available in the queue. If the
	// queue is permanently closed, it should return io.EOF error. Any
	// other error will be retried.
	Next(ctx context.Context) (RawMessage, error)
	// Done marks a specific message as completed, so that it shouldn't
	// appear on subsequent "Next" calls.
	Done(ctx context.Context, msg RawMessage) error
	// Failed marks a specific message as failed, so that it appears
	// on subsequent "Next" calls.
	Failed(ctx context.Context, msg RawMessage) error
	// Close closes the Queue. After calling this function, "Next" should
	// return io.EOF
	Close() error
}

// Stream represents an (un)bounded ordered list of messages. Typical usage
// would require one Stream for each goroutine.
type Stream interface {
	// Next should return the next message available in the queue. If the
	// queue is permanently closed, it should return io.EOF error. Any
	// other error will be retried.
	Next(ctx context.Context) (RawMessage, error)
	// Done marks all messages polled so far as complete.
	Done(ctx context.Context) error
	// Close closes the Stream. After calling this function, "Next" should
	// return io.EOF
	Close() error
}

// Processor is a basic low level message processor.
// It should be wrapped with middlewares such as logging, instrumenting,
// and so on.
type Processor interface {
	// Process handles a single message in its raw form, exactly as provided
	// from the source. If the return is an error, the engine is responsible
	// for retrying or marking the message as failed. If the return is nil,
	// the engine will mark the message as complete.
	//
	// Exact guarantees depend on the engine/stream/queue implementation, but
	// typically this method will be called at least once per message.
	// Therefore, the implementation should be idempotent.
	//
	// Depending on the engine, this method may be called concurrently.
	Process(ctx context.Context, message []byte) error
}
