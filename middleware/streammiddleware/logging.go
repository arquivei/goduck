package streammiddleware

import (
	"context"
	"time"

	"github.com/arquivei/goduck"

	"github.com/arquivei/foundationkit/errors"
	"github.com/rs/zerolog/log"
)

type streamLogging struct {
	next goduck.Stream
}

// WrapWithLogging wraps a @next with a logging that logs when methods are
// called.
func WrapWithLogging(next goduck.Stream) goduck.Stream {
	return streamLogging{
		next: next,
	}
}

func (s streamLogging) Next(ctx context.Context) (response goduck.RawMessage, err error) {
	const op = errors.Op("streammiddleware.streamLogging.Next")
	defer func(begin time.Time) {
		took := time.Since(begin)
		if err != nil {
			log.Error().
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
func (s streamLogging) Done(ctx context.Context) (err error) {
	const op = errors.Op("streammiddleware.streamLogging.Done")
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
	return s.next.Done(ctx)
}
func (s streamLogging) Close() (err error) {
	const op = errors.Op("streammiddleware.streamLogging.Close")
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
