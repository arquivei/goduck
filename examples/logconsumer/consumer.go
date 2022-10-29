package main

import (
	"context"
	"fmt"

	"github.com/arquivei/goduck"
)

// This file contains all the application specific code. Everything else is
// either handled by goduck, or is initialization code.

// Request is the endpoint-layer request. This is an user-defined type, that
// the application can understand.
type Request struct {
	Message string
}

// Decode is a  function that decodes the goduck message into the endpoint-layer request.
func Decode(ctx context.Context, message goduck.RawMessage) (interface{}, error) {
	return Request{
		// Assume that the message is a string
		Message: string(message.Bytes()),
	}, nil
}

// Do is the endpoint function that handles the request.
// @request is has the type that the decode function returns.
// The response is ignored, only the error is considered: If it is different
// from nil, the engine will try to reprocess it.
func Do(ctx context.Context, request interface{}) (interface{}, error) {
	// Extract the request into the correct type
	req := request.(Request)
	// Print the message
	fmt.Printf("Received message: %s\n", req.Message)
	return nil, nil
}
