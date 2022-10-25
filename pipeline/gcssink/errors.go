package gcssink

import "github.com/arquivei/foundationkit/errors"

var (
	// ErrInvalidSinkMessage is returned when sink message is not a gcssink.SinkMessage
	ErrInvalidSinkMessage = errors.New("invalid sink message")
	// ErrFailedToWriteAtBicket is returned when a error occurs while writing to gcs
	ErrFailedToWriteAtBucket = errors.New("failed to write the message at gcs bucket")
	// ErrFailedToCloseBucket is returned when a error occurs while closing gcs bucket
	ErrFailedToCloseBucket = errors.New("failed to close gcs bucket connection")
	// ErrFailedToCloseSink is returned when any error occurs while gcsClientGateway is writing
	ErrFailedToStoreMessages = errors.New("failed to store messages")
)
