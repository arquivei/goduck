package processormiddleware

import (
	"context"
	"math/rand"
	"time"

	"github.com/arquivei/goduck"
)

type BackoffConfig struct {
	// InitialDelay represents the delay after the first error, before adding
	// the spread
	InitialDelay time.Duration

	// MaxDelay represents the max delay after an error, before adding the
	// spread
	MaxDelay time.Duration

	// Spread is the percentage of the current delay that can be added as a
	// random term. For example, with a delay of 10s and 20% spread, the
	// calculated delay will be between 10s and 12s.
	Spread float64

	// Factor represents how bigger the next delay wil be in comparison to the
	// current one
	Factor float64
}

var DefaultBackoffConfig = BackoffConfig{
	InitialDelay: 200 * time.Millisecond,
	MaxDelay:     10 * time.Second,
	Spread:       0.2,
	Factor:       1.5,
}

type backoffMiddleware struct {
	next   goduck.Processor
	config BackoffConfig
}

// WrapWithBackoffMiddleware tries to execute @next.Process() until it
// succeeds. Each failure is followed by an exponentially increasing delay.
func WrapWithBackoffMiddleware(next goduck.Processor, config BackoffConfig) goduck.Processor {
	return backoffMiddleware{
		next:   next,
		config: config,
	}
}

func (w backoffMiddleware) Process(ctx context.Context, msg []byte) error {
	delay := w.config.InitialDelay
	err := w.next.Process(ctx, msg)
	for err != nil {
		time.Sleep(addSpread(delay, w.config.Spread))

		delay = time.Duration(float64(delay) * w.config.Factor)
		if delay > w.config.MaxDelay {
			delay = w.config.MaxDelay
		}

		err = w.next.Process(ctx, msg)
	}
	return nil
}

func addSpread(delay time.Duration, spread float64) time.Duration {
	spreadRange := int64(float64(delay.Nanoseconds()) * spread)
	return delay + time.Duration(rand.Int63n(spreadRange))*time.Nanosecond

}
