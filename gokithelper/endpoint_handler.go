package gokithelper

import (
	"context"

	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck"
	"github.com/go-kit/kit/endpoint"
)

type EndpointHandler struct {
	endpoint endpoint.Endpoint
	decoder  goduck.EndpointMessageDecoder
}

// Process func will receive the pulled message from the engine.
func (p EndpointHandler) Handle(ctx context.Context, message goduck.RawMessage) error {
	const op = errors.Op("foundationkit/goduckhelper/processor.Process")

	r, err := p.decoder(ctx, message)
	if err != nil {
		return errors.E(op, err)
	}

	_, err = p.endpoint(ctx, r)
	if err != nil {
		return errors.E(op, err)
	}

	return nil
}

// NewEndpointHandler returns a new goduck.MessageHandler that forwards the message to the endpoint.
func NewEndpointHandler(e endpoint.Endpoint, d goduck.EndpointMessageDecoder) (*EndpointHandler, error) {
	if e == nil {
		return nil, errors.E("endpoint is nil")
	}

	if d == nil {
		return nil, errors.E("decoder is nil")
	}

	return &EndpointHandler{
		endpoint: e,
		decoder:  d,
	}, nil
}
