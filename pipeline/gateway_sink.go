package pipeline

import (
	"context"

	"github.com/go-kit/kit/endpoint"
)

// Sink stores a batch of messages
type Sink interface {
	Store(ctx context.Context, input ...SinkMessage) error
}

// SinkEncoder encodes the transformation output into the Sink input type.
type SinkEncoder func(ctx context.Context, response interface{}) ([]SinkMessage, error)

// SinkMessage is the input type to the pipeline sink. This type should be an
// array
type SinkMessage interface{}

// withSink returns a decorated endpoint that sends the endpoint response to the sink.
func withSink(sink Sink, encode SinkEncoder) endpoint.Middleware {
	return func(e endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, request interface{}) (interface{}, error) {
			response, err := e(ctx, request)
			if err != nil {
				return nil, err
			}
			sinkMessages, err := encode(ctx, response)
			if err != nil {
				return nil, err
			}
			err = sink.Store(ctx, sinkMessages...)
			if err != nil {
				return nil, err
			}

			// response if not used after this point
			// but could be used by other middlewares.
			return response, nil
		}
	}
}
