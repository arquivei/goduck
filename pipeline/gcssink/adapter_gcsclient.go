package gcssink

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/arquivei/foundationkit/errors"
)

// gcsClientGateway implements GcsClientGateway interface and is responsible for
// interacting with GCS client and its objects
type gcsClientGateway struct {
	storageClient *storage.Client
}

// NewGcsClientGateway creates a new GcsClientGateway
func NewGcsGateway(client *storage.Client) GcsClientGateway {
	return &gcsClientGateway{
		storageClient: client,
	}
}

// GetWriter returns a writer to a GCS Writer that writes to the GCS object associated with the
// given bucket, object, contentType, chunkSize and retrierOption.
func (g gcsClientGateway) GetWriter(
	ctx context.Context,
	bucket, object, contentType string,
	chunkSize int,
	retrierOption ...storage.RetryOption,
) *storage.Writer {
	bucketHandler := g.storageClient.Bucket(bucket)
	obj := bucketHandler.Object(object)

	if len(retrierOption) > 0 {
		obj = obj.Retryer(retrierOption...)
	}

	writer := obj.NewWriter(ctx)
	writer.ContentType = contentType
	writer.ChunkSize = chunkSize

	return writer
}

// Write writes the given message at bucket with the given GCS Writer.
func (g gcsClientGateway) Write(writer *storage.Writer, message SinkMessage) error {
	const op = errors.Op("gcssink.gcsClientGateway.Write")

	if _, err := writer.Write(message.Data); err != nil {
		return errors.E(op, err, CodeFailedToWriteAtBucket, errors.KV("bucket", message.Bucket), errors.KV("path", message.StoragePath))
	}

	if err := writer.Close(); err != nil {
		return errors.E(op, err, CodeFailedToCloseBucket, errors.KV("bucket", message.Bucket), errors.KV("path", message.StoragePath))
	}

	return nil
}

// Close closes the GCS client
func (g gcsClientGateway) Close() error {
	return g.storageClient.Close()
}
