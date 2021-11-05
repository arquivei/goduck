package pipeline

import "errors"

var (
	// ErrShutdown is an error returned when the shutdown doesn't finishes before
	// the given context is closed.
	ErrShutdown = errors.New("shutdown didn't finished as expected")
	// ErrEndpointNil is an error returned when the endpoint is nil.
	ErrEndpointNil = errors.New("endpoint is nil")
	// ErrInputStreamNil is an error returned when the streams a nil or empty slices.
	ErrInputStreamNil = errors.New("input streams are nil or empty")
	// ErrNilDecoders is an error returned when both decoders are nil.
	ErrNilDecoders = errors.New("decoder and batch decoder are both nil")
	// ErrBothDecodersSet is an error returned when both decoders are set. Only one is allowed.
	ErrBothDecodersSet = errors.New("decoder and batch decoder cannot be set simultaneously")
	// ErrBatchSizeInvalid is an error returned when the batch size is less than 1.
	ErrBatchSizeInvalid = errors.New("invalid batch size")
	// ErrSinkNil is an error returned when the sink is not set.
	ErrSinkNil = errors.New("sink is nil")
	// ErrSinkEncoderNil is an error returned when the sink encoder is not set.
	ErrSinkEncoderNil = errors.New("sink encoder is nil")
	// ErrSystemNameEmpty is an error returned when the system name is empty.
	ErrSystemNameEmpty = errors.New("system name is empty")
)
