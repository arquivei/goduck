package implqueue

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueueShouldHandleConcurrentPools(t *testing.T) {
	nElems := 100
	queue := NewDefaultQueue(nElems)
	defer queue.Close()
	ctx := context.Background()
	done := make(chan struct{})
	for i := 0; i < nElems; i++ {
		go func(idx int) {
			msg, err := queue.Poll(ctx)
			assert.NoError(t, err, idx)
			err = queue.Done(ctx, msg)
			assert.NoError(t, err, idx)
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < nElems; i++ {
		<-done
	}
	close(done)
	assert.True(t, queue.IsEmpty())
}
func TestQueueShouldBeFinite(t *testing.T) {
	ctx := context.Background()
	nElems := 10
	queue := NewDefaultQueue(nElems)
	for i := 0; i < nElems; i++ {
		msg, _ := queue.Poll(ctx)
		queue.Done(ctx, msg)
	}
	_, err := queue.Poll(ctx)
	assert.Equal(t, io.EOF, err)
}
