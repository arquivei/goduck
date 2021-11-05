package pipeline

import (
	"context"
	"time"

	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/foundationkit/retrier"
	"github.com/rs/zerolog/log"
)

// SinkWithRetry decorates a sink with a retrier.
// It uses an exponential backoff and tries for 5 times.
// Only runtime errors are retried.
func SinkWithRetry(next Sink) Sink {
	r := retrier.NewRetrier(retrier.Settings{
		RetryEvaluator: retrier.NewGenericRetryEvaluator(retrier.GenericRetryEvaluatorSettings{
			ErrorsSeveritiesPolicy: retrier.EvaluationPolicyWhitelist,
			ErrorsSeverities:       []errors.Severity{errors.SeverityRuntime},
		}),
		BackoffCalculator: retrier.NewExponentialBackoffCalculator(retrier.ExponentialBackoffCalculatorSettings{
			BaseBackoff:        2000 * time.Millisecond,
			RandomExtraBackoff: 250 * time.Millisecond,
			Multiplier:         2.0,
		}),
		ErrorWrapper: retrier.NewLastErrorWrapper(),
	})

	return &sinkRetrier{
		next:    next,
		retrier: r,
	}
}

type sinkRetrier struct {
	next    Sink
	retrier *retrier.Retrier
}

func (s *sinkRetrier) Store(ctx context.Context, input ...SinkMessage) error {
	return s.retrier.ExecuteOperation(func() error {
		// context is canceled, just abort the operation
		// Because this error is not a runtime error, it will not be retried
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.next.Store(ctx, input...); err != nil {
			// Logs only runtime errors so we can observe individual fails
			if errors.GetSeverity(err) == errors.SeverityRuntime {
				log.Ctx(ctx).Warn().Err(err).Msg("[pipeline] Failed to send message to sink.")
			}
			return err
		}
		return nil
	})
}
