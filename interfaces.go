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
// Deprecated: use MessageHandler instead, as it uses the more generic
// RawMessage interface.
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

// MessageHandler is a basic low level message processor.
// It should be wrapped with middlewares such as logging, instrumenting,
// and so on.
type MessageHandler interface {
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
	Handle(ctx context.Context, message RawMessage) error
}

// BatchProcessor is a basic low level message processor, that is able to handle
// multiple messages at once.
// It should be wrapped with middlewares such as logging, instrumenting,
// and so on.
// Deprecated: use BatchMessageHandler instead, as it uses the more generic
// RawMessage interface.
type BatchProcessor interface {
	// Process handles a multiple messages in its raw form, exactly as provided
	// from the source. If the return is an error, the engine is responsible
	// for retrying or marking the whole batch as failed. If the return is nil,
	// the engine will mark the all the messages as complete.
	//
	// Exact guarantees depend on the engine/stream/queue implementation, but
	// typically this method will be called at least once per message.
	// Therefore, the implementation should be idempotent.
	//
	// Depending on the engine, this method may be called concurrently.
	BatchProcess(ctx context.Context, messages [][]byte) error
}

// BatchMessageHandler is a basic low level message processor, that is able to
// handle multiple messages at once.
// It should be wrapped with middlewares such as logging, instrumenting,
// and so on.
type BatchMessageHandler interface {
	// Process handles a multiple messages in its raw form, exactly as provided
	// from the source. If the return is an error, the engine is responsible
	// for retrying or marking the whole batch as failed. If the return is nil,
	// the engine will mark the all the messages as complete.
	//
	// Exact guarantees depend on the engine/stream/queue implementation, but
	// typically this method will be called at least once per message.
	// Therefore, the implementation should be idempotent.
	//
	// Depending on the engine, this method may be called concurrently.
	BatchHandle(ctx context.Context, messages []RawMessage) error
}

// AnyProcessor refer to structs that can behave both as Processor and
// BatchProcessor
type AnyProcessor interface {
	Processor
	BatchProcessor
}

// EndpointDecoder decodes a message into a endpoint request.
// See go-kit's endpoint.Endpoint.
// Deprecated: use EndpointMessageDecoder instead, as it uses the more generic
// RawMessage interface.
type EndpointDecoder func(context.Context, []byte) (interface{}, error)

// EndpointBatchDecoder decodes a message into an endpoint request.
// See go-kit's endpoint.Endpoint.
// Deprecated: use EndpointMessageBatchDecoder instead, as it uses the more
// generic RawMessage interface.
type EndpointBatchDecoder func(context.Context, [][]byte) (interface{}, error)

// EndpointMessageDecoder decodes a message into a endpoint request.
// See go-kit's endpoint.Endpoint.
type EndpointMessageDecoder func(context.Context, RawMessage) (interface{}, error)

// EndpointMessageBatchDecoder decodes a batch of messages into an endpoint request.
// See go-kit's endpoint.Endpoint.
type EndpointMessageBatchDecoder func(context.Context, []RawMessage) (interface{}, error)
