package processormiddleware

import (
	"context"

	"github.com/arquivei/goduck"
	"github.com/go-kit/kit/endpoint"
)

type endpointWrapper struct {
	next endpoint.Endpoint
}

// WrapEndpointInProcessor creates a Processor/BatchProcessor from a go-kit endpoint
func WrapEndpointInProcessor(next endpoint.Endpoint) goduck.AnyProcessor {
	return endpointWrapper{next: next}
}

func (w endpointWrapper) BatchProcess(ctx context.Context, msg [][]byte) error {
	_, err := w.next(ctx, msg)
	return err
}

func (w endpointWrapper) Process(ctx context.Context, msg []byte) error {
	_, err := w.next(ctx, msg)
	return err
}
