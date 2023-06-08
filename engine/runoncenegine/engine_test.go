package runoncenegine_test

import (
	"context"
	"testing"
	"time"

	"github.com/arquivei/goduck/engine/runoncenegine"
	"github.com/arquivei/goduck/impl/implprocessor"
	"github.com/arquivei/goduck/impl/implstream"

	"github.com/arquivei/foundationkit/errors"
	"github.com/stretchr/testify/assert"
)

func TestStream(t *testing.T) {
	processor := implprocessor.New(nil)
	stream := implstream.NewDefaultStream(0, 100)
	defer func() {
		stream.Close()
	}()

	w := runoncenegine.New(processor, 101, 100*time.Millisecond, stream)
	err := w.Run(context.Background())
	assert.NoError(t, err)

	assert.Equal(t, 100, len(processor.Success))
}

func TestStreamNoTimeout(t *testing.T) {
	processor := implprocessor.New(nil)
	stream := implstream.NewDefaultStream(0, 100)

	defer func() {
		stream.Close()
	}()

	w := runoncenegine.New(processor, 15, 0, stream)
	err := w.Run(context.Background())
	assert.NoError(t, err)

	assert.Equal(t, 15, len(processor.Success))
}

// TestStreamCancel asserts that the engine stops when context is close
func TestStreamCancel(t *testing.T) {
	wait := make(chan struct{})
	done := make(chan struct{})
	processor := implprocessor.New(func() error {
		<-wait
		return nil
	})
	stream := implstream.NewDefaultStream(0, 100)

	defer func() {
		stream.Close()
	}()

	ctx, cancelFn := context.WithCancel(context.Background())
	w := runoncenegine.New(processor, 10, 100*time.Millisecond, stream)
	go func() {
		w.Run(ctx)
		close(done)
	}()
	cancelFn()
	close(wait)
	<-done
}

// TestStreamFatal asserts that the engine stops when the processor returns a
// fatal error
func TestStreamFatal(t *testing.T) {
	expectedErr := errors.E("my error", errors.SeverityFatal)

	processor := implprocessor.New(func() error {
		return expectedErr
	})

	stream := implstream.NewDefaultStream(0, 100)

	defer func() {
		stream.Close()
	}()

	w := runoncenegine.New(processor, 10, 100*time.Millisecond, stream)
	err := w.Run(context.Background())
	assert.Equal(t, expectedErr, err)
}
