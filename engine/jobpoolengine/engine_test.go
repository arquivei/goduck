package jobpoolengine_test

import (
	"context"
	"sync"
	"testing"

	"github.com/arquivei/goduck/engine/jobpoolengine"
	"github.com/arquivei/goduck/impl/implprocessor"
	"github.com/arquivei/goduck/impl/implqueue"
	"github.com/stretchr/testify/assert"
)

func TestJobPool(t *testing.T) {
	nWorkers := 5
	processor := &implprocessor.MockProcessor{
		Mtx:      &sync.Mutex{},
		Success:  map[string]bool{},
		CustomFn: func() {},
	}
	queue := implqueue.NewDefaultQueue(100)
	defer queue.Close()
	w := jobpoolengine.New(queue, processor, nWorkers)
	w.Run(context.Background())

	assert.Equal(t, 100, len(processor.Success))

}

// TestJobPoolCancel asserts that the engine stops when context is closed
func TestJobPoolCancel(t *testing.T) {
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
	queue := implqueue.NewDefaultQueue(100)
	defer queue.Close()

	ctx, cancelFn := context.WithCancel(context.Background())
	w := jobpoolengine.New(queue, processor, nWorkers)
	go func() {
		w.Run(ctx)
		close(done)
	}()
	cancelFn()
	close(wait)
	<-done
}

// TestJobPoolPanic tests that the engine is resilient to panics
func TestJobPoolPanic(t *testing.T) {
	nWorkers := 1
	idx := 0
	processor := &implprocessor.MockProcessor{
		Mtx:     &sync.Mutex{},
		Success: map[string]bool{},
		CustomFn: func() {
			idx++
			if idx%5 == 0 {
				panic("explodes")
			}
		},
	}
	queue := implqueue.NewDefaultQueue(100)
	defer queue.Close()
	w := jobpoolengine.New(queue, processor, nWorkers)
	w.Run(context.Background())

	assert.Equal(t, 100, len(processor.Success))

}
