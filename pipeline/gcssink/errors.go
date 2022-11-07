package gcssink

import "github.com/arquivei/foundationkit/errors"

var (
	// CodeWrongTypeSinkMessage is returned when sink message is not a gcssink.SinkMessage
	CodeWrongTypeSinkMessage = errors.Code("WRONG_TYPE_SINK_MESSAGE")
	// CodeEmptyDataSinkMessage is returned when sink message data is empty
	CodeEmptyDataSinkMessage = errors.Code("EMPTY_DATA_MESSAGE")
	// CodeFailedToWriteAtBucket is returned when a error occurs while writing to gcs
	CodeFailedToWriteAtBucket = errors.Code("FAILED_TO_WRITE_AT_BUCKET")
	// CodeFailedToCloseBucket is returned when a error occurs while closing gcs bucket
	CodeFailedToCloseBucket = errors.Code("FAILED_TO_CLOSE_BUCKET")
	// CodePanic is returned when a panic occurs
	CodePanic = errors.Code("PANIC_TO_WRITE_AT_BUCKET")

	// ErrFailedToCloseSink is returned when any error occurs while gcsClientGateway is writing
	ErrFailedToStoreMessages = errors.New("failed to store messages")
	// ErrInvalidMessage is returned when sink message is invalid
	ErrInvalidSinkMessage = errors.New("invalid sink message")
	//ErrPanic is returned when a panic occurs
	ErrPanic = errors.New("panic occurred while storing to gcs")
)
