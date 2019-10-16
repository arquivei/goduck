package processormiddleware

import (
	"context"
	"time"

	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck"
)

type timeoutMiddleware struct {
	next    goduck.Processor
	timeout time.Duration
}

func WrapWithTimeout(next goduck.Processor, timeout time.Duration) goduck.Processor {
	return timeoutMiddleware{next, timeout}
}

func (m timeoutMiddleware) Process(ctx context.Context, message []byte) error {
	const op = errors.Op("timeoutMiddleware.Process")
	ctx, cancelFn := context.WithTimeout(ctx, m.timeout)
	defer cancelFn()
	err := m.next.Process(ctx, message)
	if ctx.Err() != nil {
		return errors.E(op, ctx.Err())
	}
	return errors.E(op, err)
}
