package batchstreamengine_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/arquivei/goduck"
	"github.com/arquivei/goduck/engine/batchstreamengine"
	"github.com/arquivei/goduck/impl/implprocessor"
	"github.com/arquivei/goduck/impl/implstream"

	"github.com/arquivei/foundationkit/errors"
	"github.com/stretchr/testify/assert"
)

func TestStream(t *testing.T) {
	nWorkers := 5
	processor := implprocessor.New(nil)
	streams := make([]goduck.Stream, nWorkers)
	for i := 0; i < nWorkers; i++ {
		streams[i] = implstream.NewDefaultStream(i, 100)
	}
	defer func() {
		for _, stream := range streams {
			stream.Close()
		}
	}()

	w := batchstreamengine.New(processor, 11, 100*time.Millisecond, streams)
	w.Run(context.Background())

	assert.Equal(t, 500, len(processor.Success))

}

func TestStreamNoTimeout(t *testing.T) {
	nWorkers := 5
	processor := implprocessor.New(nil)
	streams := make([]goduck.Stream, nWorkers)
	for i := 0; i < nWorkers; i++ {
		streams[i] = implstream.NewDefaultStream(i, 100)
	}
	defer func() {
		for _, stream := range streams {
			stream.Close()
		}
	}()

	w := batchstreamengine.New(processor, 15, 0, streams)
	w.Run(context.Background())

	assert.Equal(t, 500, len(processor.Success))

}

// TestStreamCancel asserts that the engine stops when context is close
func TestStreamCancel(t *testing.T) {
	nWorkers := 5
	wait := make(chan struct{})
	done := make(chan struct{})
	processor := implprocessor.New(func() error {
		<-wait
		return nil
	})
	streams := make([]goduck.Stream, nWorkers)
	for i := 0; i < nWorkers; i++ {
		streams[i] = implstream.NewDefaultStream(i, 100)
	}
	defer func() {
		for _, stream := range streams {
			stream.Close()
		}
	}()

	ctx, cancelFn := context.WithCancel(context.Background())
	w := batchstreamengine.New(processor, 10, 100*time.Millisecond, streams)
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
	nWorkers := 5
	failAfter := 10
	expectedErr := errors.E("my error", errors.SeverityFatal)

	count := 0
	countMtx := &sync.Mutex{}
	processor := implprocessor.New(func() error {
		countMtx.Lock()
		defer countMtx.Unlock()
		count++
		if count >= failAfter {
			return expectedErr
		}
		return nil
	})
	streams := make([]goduck.Stream, nWorkers)
	for i := 0; i < nWorkers; i++ {
		streams[i] = implstream.NewDefaultStream(i, 100)
	}
	defer func() {
		for _, stream := range streams {
			stream.Close()
		}
	}()
	w := batchstreamengine.New(processor, 10, 100*time.Millisecond, streams)
	err := w.Run(context.Background())
	assert.Equal(t, expectedErr, err)
}
