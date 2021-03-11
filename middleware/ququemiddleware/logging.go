package ququemiddleware

import (
	"context"
	"time"

	"github.com/arquivei/goduck"

	"github.com/arquivei/foundationkit/errors"
	"github.com/rs/zerolog/log"
)

type poolLogging struct {
	next goduck.MessagePool
}

// WrapWithLogging wraps a @next with a logging that logs when methods are
// called.
func WrapWithLogging(next goduck.MessagePool) goduck.MessagePool {
	return poolLogging{
		next: next,
	}
}

func (s poolLogging) Next(ctx context.Context) (response goduck.RawMessage, err error) {
	const op = errors.Op("ququemiddleware.poolLogging.Next")
	defer func(begin time.Time) {
		took := time.Since(begin)
		if err != nil {
			logger := log.Error()
			if ctx.Err() != nil {
				logger = log.Debug()
			}
			logger.
				Dur("took", took).
				Err(errors.E(op, err)).
				Msg("Error fetching next message")
		} else {
			log.Debug().
				Dur("took", took).
				Int("size", len(response.Bytes())).
				Msg("Successfully fetched next message")
		}

	}(time.Now())
	return s.next.Next(ctx)

}
func (s poolLogging) Done(ctx context.Context, msg goduck.RawMessage) (err error) {
	const op = errors.Op("ququemiddleware.poolLogging.Done")
	defer func(begin time.Time) {
		took := time.Since(begin)
		if err != nil {
			log.Error().
				Dur("took", took).
				Err(errors.E(op, err)).
				Msg("Error marking messages as done")
		} else {
			log.Debug().
				Dur("took", took).
				Msg("Successfully acked messages")
		}
	}(time.Now())
	return s.next.Done(ctx, msg)
}

func (s poolLogging) Failed(ctx context.Context, msg goduck.RawMessage) (err error) {
	const op = errors.Op("ququemiddleware.poolLogging.Failed")
	defer func(begin time.Time) {
		took := time.Since(begin)
		if err != nil {
			log.Error().
				Dur("took", took).
				Err(errors.E(op, err)).
				Msg("Error marking messages as failed")
		} else {
			log.Debug().
				Dur("took", took).
				Msg("Successfully acked messages")
		}
	}(time.Now())
	return s.next.Failed(ctx, msg)
}
func (s poolLogging) Close() (err error) {
	const op = errors.Op("ququemiddleware.poolLogging.Close")
	defer func(begin time.Time) {
		took := time.Since(begin)
		if err != nil {
			log.Error().
				Dur("took", took).
				Err(errors.E(op, err)).
				Msg("Error closing the stream")
		} else {
			log.Debug().
				Dur("took", took).
				Msg("Successfully closed the stream")
		}
	}(time.Now())
	return s.next.Close()
}
