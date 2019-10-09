package engine

import (
	"context"

	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck"
)

func SafeProcess(ctx context.Context, processor goduck.Processor, msg []byte) (err error) {
	const op = errors.Op("engine.SafeProcess")
	defer func() {
		if r := recover(); r != nil {
			err = errors.E(op, r)
		}
	}()
	return processor.Process(ctx, msg)
}
