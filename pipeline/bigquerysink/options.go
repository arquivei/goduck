package bigquerysink

import "github.com/rs/zerolog"

// Option configures the bigquery sink.
type Option func(*bigquerySink)

// WithLogLevel changes the bigquery allowed log level.
// The bigquery sink will emit log messages bellow this level.
// The default value is zerolog.Disabled. No log is emitted.
// Setting it to zerolog.Trace will enable all messages.
func WithLogLevel(l zerolog.Level) Option {
	return func(bs *bigquerySink) {
		bs.logLevel = l
	}
}

// WithIgnoreUnkownMessageTypes will make the bigquery sink
// ignore messages that are not of type Message.
// The default behavior is return an error if the message is
// not of the expected type.
func WithIgnoreUnkownMessageTypes() Option {
	return func(bs *bigquerySink) {
		bs.shouldIgnoreUnkownMessages = true
	}
}

// WithIgnoreValidationErrors will make the bigquery sink ignore
// incomplete messages. Otherwise it will return an error.
func WithIgnoreValidationErrors() Option {
	return func(bs *bigquerySink) {
		bs.shouldIgnoreValidationErrors = true
	}
}
