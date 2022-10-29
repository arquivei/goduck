package streamengine_test

import (
	"context"
	"sync"
	"testing"

	"github.com/arquivei/goduck"
	"github.com/arquivei/goduck/engine/streamengine/v2"
	"github.com/arquivei/goduck/impl/implstream"

	"github.com/arquivei/foundationkit/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestStream(t *testing.T) {
	nWorkers := 5

	handler := &MessageHandlerMock{}

	counter := 0
	counterMutex := &sync.Mutex{}

	handler.On("Handle", mock.Anything).Return(func() error {
		counterMutex.Lock()
		defer counterMutex.Unlock()
		counter++
		if counter%3 == 0 {
			return errors.New("intermitent error")
		}
		return nil
	}).Times(749)

	streams := make([]goduck.Stream, nWorkers)
	for i := 0; i < nWorkers; i++ {
		streams[i] = implstream.NewDefaultStream(i, 100)
	}
	defer func() {
		for _, stream := range streams {
			stream.Close()
		}
	}()

	w := streamengine.New(handler, streams)
	err := w.Run(context.Background())
	assert.NoError(t, err)
	handler.AssertExpectations(t)
}

// TestStreamCancel asserts that the engine stops when context is close
func TestStreamCancel(t *testing.T) {
	nWorkers := 1
	wait := make(chan struct{})
	wait2 := make(chan struct{})

	handler := &MessageHandlerMock{}
	handler.On("Handle", mock.Anything).Return(nil).Times(10)
	handler.On("Handle", mock.Anything).Return(nil).Once().Run(func(args mock.Arguments) {
		close(wait)
		<-wait2
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
	w := streamengine.New(handler, streams)
	go func() {
		<-wait
		cancelFn()
		close(wait2)
	}()

	err := w.Run(ctx)
	assert.NoError(t, err)
	handler.AssertExpectations(t)
}

// TestStreamFatal asserts that the engine stops when the processor returns a
// fatal error
func TestStreamFatal(t *testing.T) {
	nWorkers := 5
	failAfter := 10
	expectedErr := errors.E("my error", errors.SeverityFatal)

	handler := &MessageHandlerMock{}
	handler.On("Handle", mock.Anything).Return(nil).Times(failAfter)
	handler.On("Handle", mock.Anything).Return(expectedErr)

	streams := make([]goduck.Stream, nWorkers)
	for i := 0; i < nWorkers; i++ {
		streams[i] = implstream.NewDefaultStream(i, 100)
	}
	defer func() {
		for _, stream := range streams {
			stream.Close()
		}
	}()

	w := streamengine.New(handler, streams)
	err := w.Run(context.Background())
	assert.Equal(t, expectedErr, err)
	handler.AssertExpectations(t)
}

type MessageHandlerMock struct {
	mock.Mock
}

func (m *MessageHandlerMock) Handle(ctx context.Context, message goduck.RawMessage) error {
	args := m.Called(message)
	result := args.Get(0)

	if err, ok := result.(error); ok {
		return err
	}
	if errFn, ok := result.(func() error); ok {
		return errFn()
	}
	return nil
}
