package gcssink

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/arquivei/foundationkit/errors"
)

type gcsClientGateway struct {
	storageClient *storage.Client
}

func NewGcsGateway(client *storage.Client) GcsClientGateway {
	return &gcsClientGateway{
		storageClient: client,
	}
}

func (g gcsClientGateway) GetWriter(
	ctx context.Context,
	bucket, object, contentType string,
	chunkSize int,
	retrierConfig []storage.RetryOption,
) *storage.Writer {
	bucketHandler := g.storageClient.Bucket(bucket)
	obj := bucketHandler.Object(object)

	if len(retrierConfig) > 0 {
		obj = obj.Retryer(retrierConfig...)
	}

	writer := obj.NewWriter(ctx)
	writer.ContentType = contentType
	writer.ChunkSize = chunkSize

	return writer
}

func (g gcsClientGateway) Write(writer *storage.Writer, message SinkMessage) error {
	const op = errors.Op("gcssink.gcsClientGateway.Write")

	if message.Data == nil {
		return errors.E(op, ErrInvalidSinkMessage)
	}

	if _, err := writer.Write(message.Data); err != nil {
		return errors.E(op, ErrFailedToWriteAtBucket, errors.KV("bucket", message.Bucket), errors.KV("path", message.StoragePath), errors.KV("error", err))
	}

	if err := writer.Close(); err != nil {
		return errors.E(op, ErrFailedToCloseBucket, errors.KV("bucket", message.Bucket), errors.KV("path", message.StoragePath), errors.KV("error", err))
	}

	return nil
}

func (g gcsClientGateway) Close() error {
	return g.storageClient.Close()
}
