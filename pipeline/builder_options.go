package pipeline

import (
	"time"

	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck"
	"github.com/go-kit/kit/endpoint"
	"github.com/rs/zerolog/log"
)

// pipelineBuilderOptions is the configuration of a pipeline.
type pipelineBuilderOptions struct {
	// endpoint that executes the main business logic
	endpoint endpoint.Endpoint

	// GoDuck configuration:
	//
	// inputStreams are the message sources
	inputStreams []goduck.Stream

	// messagePool, if set, makes the pipeline uses a job pool engine to fetch the messages.
	messagePool goduck.MessagePool

	// nPoolWorkers is the number of workers consuming from the job pool.
	nPoolWorkers int

	// Decoders converts the messages from the input stream to an endpoint request.
	// Only one of the following decoders is allowed. This affects with kind of engine will be used.
	//
	// batchDecoder decodes the stream in batches. Implies batchstreamengine.
	batchDecoder goduck.EndpointBatchDecoder
	// batchSize is the size of the batch when using batchstreamengine.
	// Defaults to 1.
	batchSize int
	// decoder decodes the stream one message at a time. Implies streamengine.
	decoder goduck.EndpointDecoder

	// maxTimeout is the timeout when fetching messages from the stream
	maxTimeout time.Duration

	// sink configuration:
	//
	// sink handles the endpoint responses. Could be publishing to a topic of saving into a database.
	sink Sink
	// sinkEncoder encodes an endpoint response into a SinkMessage
	sinkEncoder SinkEncoder

	// dlq handles errors that have been retried and failed
	dlq struct {
		brokers  []string
		topic    string
		username string
		password string
	}

	middlewares []endpoint.Middleware
}

func checkPipelineBuilderOptions(c pipelineBuilderOptions) error {
	const op = errors.Op("checkPipelineBuilderOptions")

	if c.endpoint == nil {
		return errors.E(op, ErrEndpointNil)
	}

	if c.messagePool == nil && len(c.inputStreams) == 0 {
		return errors.E(op, ErrEmptyInputStreamOrMessagePool)
	}

	if c.messagePool != nil && len(c.inputStreams) > 1 {
		return errors.E(op, ErrBothInputSet)
	}

	if c.decoder == nil && c.batchDecoder == nil {
		return errors.E(op, ErrNilDecoders)
	}

	if c.decoder != nil && c.batchDecoder != nil {
		return errors.E(op, ErrBothDecodersSet)
	}

	if c.batchDecoder != nil && c.batchSize < 1 {
		return errors.E(op, ErrBatchSizeInvalid)
	}

	if c.sink == nil {
		return errors.E(op, ErrSinkNil)
	}

	if c.sinkEncoder == nil {
		return errors.E(op, ErrSinkEncoderNil)
	}

	if c.dlq.brokers == nil {
		log.Warn().Msg("No DLQ Topic is set, all messages will be retried forever.")
	}
	return nil
}
