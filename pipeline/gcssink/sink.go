package gcssink

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck/pipeline"
)

// GcsClientGateway represents a gateway to GCS client
type GcsClientGateway interface {
	// GetWriter returns a writer to a GCS object with the given bucket, object, contentType, chunkSize and retrierConfig.
	// If retrierConfig is not empty, the writer will be configured with the given retry options.
	GetWriter(ctx context.Context, bucket, object, contentType string, chunkSize int, retrierConfig []storage.RetryOption) *storage.Writer
	// Write writes the given message to the given writer.
	Write(writer *storage.Writer, message SinkMessage) error
	// Close closes the GCS client
	Close() error
}

// gcsParallelWriter is a pipeline.Sink that writes messages to GCS in parallel.
type gcsParallelWriter struct {
	clientGateway GcsClientGateway
	contentType   string
	chunkSize     int
	// retrierConfig is the configuration for the retrier. It is used to
	// configure the retry police for the GCS client. Storage sdk provides
	// some functions to create the retrier config. See:
	// https://pkg.go.dev/cloud.google.com/go/storage#RetryOption
	retrierConfig []storage.RetryOption
}

// SinkMessage is the input for the GCS Sink
type SinkMessage struct {
	Data        []byte
	StoragePath string
	Bucket      string
	Metadata    map[string]string
}

// MustNew creates a new pipeline sink that write messages to GCS. It panics if
// GcsClientGateway, contentType or chunkSize are nil or invalid. Order of the messages is not guaranteed.
func MustNewParallel(
	clientGateway GcsClientGateway,
	contentType string,
	chunkSize int,
	retrierConfig []storage.RetryOption,
) (pipeline.Sink, func() error) {
	if clientGateway == nil {
		panic("clientGateway is nil")
	}

	if contentType == "" {
		panic("missing content type")
	}

	if chunkSize <= 0 {
		panic("invalid chunk size")
	}

	writer := &gcsParallelWriter{
		clientGateway: clientGateway,
		contentType:   contentType,
		chunkSize:     chunkSize,
		retrierConfig: retrierConfig,
	}

	closeFn := func() error {
		return clientGateway.Close()
	}

	return writer, closeFn
}

// Store implements the pipeline.Sink interface. It writes a batch of messages in parallel.
// If any of the messages fail to be written, it returns an error. Order of the messages is not guaranteed.
func (w *gcsParallelWriter) Store(ctx context.Context, messages ...pipeline.SinkMessage) error {
	const op = errors.Op("gcssink.gcsParallelWriter.Store")

	wg := &sync.WaitGroup{}

	if len(messages) == 0 || messages == nil {
		return nil
	}

	errs := make(chan error, len(messages))
	for _, msg := range messages {
		wg.Add(1)
		go func(msg pipeline.SinkMessage) {
			defer wg.Done()

			sinkMsg, ok := msg.(SinkMessage)
			if !ok {
				errs <- errors.E(ErrInvalidSinkMessage, errors.KV("type", fmt.Sprintf("%T", msg)))
				return
			}

			writer := w.clientGateway.GetWriter(ctx, sinkMsg.Bucket, sinkMsg.StoragePath, w.contentType, w.chunkSize, w.retrierConfig)
			errs <- w.clientGateway.Write(writer, sinkMsg)

		}(msg)
	}

	wg.Wait()

	var sliceErrs []error
	for range messages {
		if e := <-errs; e != nil {
			sliceErrs = append(sliceErrs, e)
		}
	}

	close(errs) // TODO: check if this should be here

	if len(sliceErrs) > 0 {
		return errors.E(op, ErrFailedToStoreMessages, errors.KV("errors", sliceErrs))
	}

	return nil
}
