package processormiddleware

import (
	"context"
	"testing"
	"time"

	"github.com/arquivei/foundationkit/errors"
	"github.com/stretchr/testify/assert"
)

func TestRunWithBackoffSuccess(t *testing.T) {
	count := 0
	runnable := func(_ context.Context) error {
		count++
		if count < 3 {
			return errors.E("random error")
		}
		return nil
	}
	err := runWithBackoff(context.Background(), DefaultBackoffConfig, runnable)
	assert.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestRunWithBackoffContextCancelledDuringSleep(t *testing.T) {
	runnable := func(_ context.Context) error {
		return errors.E("random error")
	}
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second)
	defer cancelFn()
	err := runWithBackoff(ctx, DefaultBackoffConfig, runnable)
	assert.EqualError(t, err, ctx.Err().Error())
}

func TestRunWithBackoffContextCancelledDuringProcessing(t *testing.T) {
	runnable := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Second)
	defer cancelFn()
	err := runWithBackoff(ctx, DefaultBackoffConfig, runnable)
	assert.EqualError(t, err, ctx.Err().Error())
}
