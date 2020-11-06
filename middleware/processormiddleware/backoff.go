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

type processorBackoffMiddleware struct {
	next   goduck.Processor
	config BackoffConfig
}

type batchProcessorBackoffMiddleware struct {
	next   goduck.BatchProcessor
	config BackoffConfig
}

// WrapWithBackoffMiddleware tries to execute @next.Process() until it
// succeeds. Each failure is followed by an exponentially increasing delay.
func WrapWithBackoffMiddleware(next goduck.Processor, config BackoffConfig) goduck.Processor {
	return processorBackoffMiddleware{
		next:   next,
		config: config,
	}
}

// WrapBatchProcessorWithBackoffMiddleware tries to execute @next.BatchProcess() until it
// succeeds. Each failure is followed by an exponentially increasing delay.
func WrapBatchProcessorWithBackoffMiddleware(next goduck.BatchProcessor, config BackoffConfig) goduck.BatchProcessor {
	return batchProcessorBackoffMiddleware{
		next:   next,
		config: config,
	}
}

func (w batchProcessorBackoffMiddleware) BatchProcess(ctx context.Context, msg []goduck.Message) error {
	return runWithBackoff(ctx, w.config, func(ctx context.Context) error {
		return w.next.BatchProcess(ctx, msg)
	})
}

func (w processorBackoffMiddleware) Process(ctx context.Context, msg goduck.Message) error {
	return runWithBackoff(ctx, w.config, func(ctx context.Context) error {
		return w.next.Process(ctx, msg)
	})
}

func runWithBackoff(ctx context.Context, config BackoffConfig, runnable func(context.Context) error) error {
	delay := config.InitialDelay
	err := runnable(ctx)
	for err != nil {
		amountToSleep := addSpread(delay, config.Spread)

		waitCtx, cancelFn := context.WithTimeout(context.Background(), amountToSleep)
		defer cancelFn()

		select {
		case <-waitCtx.Done():
		case <-ctx.Done():
			return ctx.Err()
		}

		delay = time.Duration(float64(delay) * config.Factor)
		if delay > config.MaxDelay {
			delay = config.MaxDelay
		}

		err = runnable(ctx)
	}
	return nil
}

func addSpread(delay time.Duration, spread float64) time.Duration {
	spreadRange := int64(float64(delay.Nanoseconds()) * spread)
	return delay + time.Duration(rand.Int63n(spreadRange))*time.Nanosecond

}
