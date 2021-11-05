package noopsink

import (
	"context"

	"github.com/arquivei/goduck/pipeline"
)

type noop struct {
}

func (e *noop) Store(context.Context, ...pipeline.SinkMessage) error {
	return nil
}

// New creates a new sink that does nothing
func New() pipeline.Sink {
	return &noop{}
}
