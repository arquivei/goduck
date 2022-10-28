package gcssink

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck/pipeline"
	"golang.org/x/sync/errgroup"
)

// GcsClientGateway represents a gateway to GCS client
type GcsClientGateway interface {
	// GetWriter returns a writer to a GCS object with the given bucket, object, contentType, chunkSize and retrierOption.
	// If retrierOption is not empty, the writer will be configured with the given retry options.
	GetWriter(ctx context.Context, bucket, object, contentType string, chunkSize int, retrierOption ...storage.RetryOption) *storage.Writer
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
	// retrierOption is the configuration for the retrier. It is used to
	// configure the retry police for the GCS client. Storage sdk provides
	// some functions to create the retrier config. See:
	// https://pkg.go.dev/cloud.google.com/go/storage#RetryOption
	retrierOption []storage.RetryOption
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
	retrierOption []storage.RetryOption,
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
		retrierOption: retrierOption,
	}

	return writer, clientGateway.Close
}

// Store implements the pipeline.Sink interface. It writes a batch of messages in parallel.
// If any of the messages fail to be written, it returns an error. Order of the messages is not guaranteed.
func (w *gcsParallelWriter) Store(ctx context.Context, messages ...pipeline.SinkMessage) error {
	const op = errors.Op("gcssink.gcsParallelWriter.Store")

	if len(messages) == 0 || messages == nil {
		return nil
	}

	g := &errgroup.Group{}
	errChan := make(chan error, len(messages))

	for _, message := range messages {
		message := message
		g.Go(func() error {
			sinkMsg, ok := message.(SinkMessage)
			if !ok {
				errChan <- errors.E(ErrInvalidSinkMessage, CodeWrongTypeSinkMessage)
				return nil
			}

			if sinkMsg.Data == nil {
				errChan <- errors.E(ErrInvalidSinkMessage, CodeEmptyDataSinkMessage)
				return nil
			}

			writer := w.clientGateway.GetWriter(ctx, sinkMsg.Bucket, sinkMsg.StoragePath, w.contentType, w.chunkSize, w.retrierOption...)
			errChan <- w.clientGateway.Write(writer, sinkMsg)

			return nil
		})
	}

	g.Wait()

	var sliceErrs []error
	for range messages {
		if e := <-errChan; e != nil {
			sliceErrs = append(sliceErrs, e)
		}
	}

	close(errChan)

	if len(sliceErrs) > 0 {
		return errors.E(op, ErrFailedToStoreMessages, errors.KV("errors", sliceErrs))
	}

	return nil
}
