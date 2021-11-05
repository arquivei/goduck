package inputstreams

import "errors"

var (
	// ErrEmptyKafkaTopic is returned when the provided topic is an empty string.
	ErrEmptyKafkaTopic = errors.New("empty kafka topic")
	// ErrNoKafkaTopic is returned when the topics are not set.
	ErrNoKafkaTopic = errors.New("no kafka topic provided")
)
