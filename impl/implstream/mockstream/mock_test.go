package mockstream

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStream(t *testing.T) {
	nElems := 100
	queue := NewDefaultStream(0, nElems)
	ctx := context.Background()
	defer queue.Close()

	for i := 0; i < nElems; i++ {
		_, err := queue.Next(ctx)
		assert.NoError(t, err, i)
		err = queue.Done(ctx)
		assert.NoError(t, err, i)
	}
	assert.True(t, queue.IsEmpty())
	_, err := queue.Next(ctx)
	assert.Equal(t, io.EOF, err)
}
