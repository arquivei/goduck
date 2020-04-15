package jobpoolengine_test

import (
	"context"
	"sync"
	"testing"

	"github.com/arquivei/goduck/engine/jobpoolengine"
	"github.com/arquivei/goduck/impl/implprocessor"
	"github.com/arquivei/goduck/impl/implqueue"

	"github.com/arquivei/foundationkit/errors"
	"github.com/stretchr/testify/assert"
)

func TestJobPool(t *testing.T) {
	nWorkers := 5
	processor := implprocessor.New(nil)
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
	processor := implprocessor.New(func() error {
		<-wait
		return nil
	})
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
	queue := implqueue.NewDefaultQueue(100)
	defer queue.Close()

	w := jobpoolengine.New(queue, processor, nWorkers)
	err := w.Run(context.Background())
	assert.Equal(t, expectedErr, err)
}
