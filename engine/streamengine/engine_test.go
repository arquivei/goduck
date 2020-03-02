package streamengine_test

import (
	"context"
	"sync"
	"testing"

	"github.com/arquivei/goduck"
	"github.com/arquivei/goduck/engine/streamengine"
	"github.com/arquivei/goduck/impl/implprocessor"
	"github.com/arquivei/goduck/impl/implstream"
	"github.com/stretchr/testify/assert"
)

func TestStream(t *testing.T) {
	nWorkers := 5
	processor := &implprocessor.MockProcessor{
		Mtx:      &sync.Mutex{},
		Success:  map[string]bool{},
		CustomFn: func() {},
	}
	streams := make([]goduck.Stream, nWorkers)
	for i := 0; i < nWorkers; i++ {
		streams[i] = implstream.NewDefaultStream(i, 100)
	}
	defer func() {
		for _, stream := range streams {
			stream.Close()
		}
	}()

	w := streamengine.New(processor, streams)
	w.Run(context.Background())

	assert.Equal(t, 500, len(processor.Success))

}

// TestStreamCancel asserts that the engine stops when context is close
func TestStreamCancel(t *testing.T) {
	nWorkers := 5
	wait := make(chan struct{})
	done := make(chan struct{})
	processor := &implprocessor.MockProcessor{
		Mtx:     &sync.Mutex{},
		Success: map[string]bool{},
		CustomFn: func() {
			<-wait
		},
	}
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
	w := streamengine.New(processor, streams)
	go func() {
		w.Run(ctx)
		close(done)
	}()
	cancelFn()
	close(wait)
	<-done
}
