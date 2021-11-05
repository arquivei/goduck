package inputstreams

import (
	"context"
	"errors"
	"testing"

	"github.com/arquivei/foundationkit/app"
	"github.com/arquivei/goduck"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockProvider struct {
	mock.Mock
}

func (p *mockProvider) MakeStream() (goduck.Stream, error) {
	args := p.Called()
	s, _ := args.Get(0).(goduck.Stream)
	return s, args.Error(1)
}

type mockStream struct {
	mock.Mock
}

func (s *mockStream) Next(ctx context.Context) (goduck.RawMessage, error) {
	args := s.Called()
	return args.Get(0).(goduck.RawMessage), args.Error(1)
}

func (s *mockStream) Done(ctx context.Context) error {
	args := s.Called()
	return args.Error(0)
}

func (s *mockStream) Close() error {
	args := s.Called()
	return args.Error(0)
}

func withMockProvider(p *mockProvider) Option {
	return func(o *options) error {
		o.provider = p
		return nil
	}
}

func TestNew(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	p := &mockProvider{}
	p.On("MakeStream").Return(&mockStream{}, nil)

	t.Run("Error: No provider", func(t *testing.T) {
		s, err := New()
		assert.Nil(t, s)
		assert.EqualError(t, err, ErrNoProvider.Error())
	})

	t.Run("Error: Missing number of streams", func(t *testing.T) {
		s, err := New(
			withMockProvider(&mockProvider{}),
			WithNumberOfStreams(-1),
		)
		assert.Nil(t, s)
		assert.EqualError(t, err, ErrZeroStreams.Error())
	})

	t.Run("Error: failed to create input stream", func(t *testing.T) {
		mp := &mockProvider{}
		mp.On("MakeStream").Return(nil, errors.New("some error"))

		s, err := New(
			withMockProvider(mp),
		)
		assert.Nil(t, s)
		assert.EqualError(t, err, "some error")
	})

	// We must create a default app so the code can register the shutdown handlers.
	err := app.NewDefaultApp(context.Background())
	if err != nil {
		panic(err)
	}

	t.Run("Success: No app and no number of streams", func(t *testing.T) {
		s, err := New(
			withMockProvider(p),
		)
		assert.NotNil(t, s)
		assert.NoError(t, err)
	})

	t.Run("Success: No app", func(t *testing.T) {
		s, err := New(
			withMockProvider(p),
			WithNumberOfStreams(2),
		)
		assert.NotNil(t, s)
		assert.NoError(t, err)
	})

	t.Run("Success - with normal app", func(t *testing.T) {
		ms := &mockStream{}
		ms.On("Close").Return(nil)

		mp := &mockProvider{}
		mp.On("MakeStream").Return(ms, nil)

		myapp, err := app.New(context.Background(), "9002")
		if !assert.NoError(t, err) {
			return
		}

		s, err := New(
			withMockProvider(mp),
			WithNumberOfStreams(1),
			WithApp(myapp),
		)
		assert.NotNil(t, s)
		assert.NoError(t, err)

		err = myapp.Shutdown(context.Background())
		assert.NoError(t, err)

		ms.AssertExpectations(t)
		mp.AssertExpectations(t)
	})

	t.Run("Success - with default app", func(t *testing.T) {
		ms := &mockStream{}
		ms.On("Close").Return(nil)

		mp := &mockProvider{}
		mp.On("MakeStream").Return(ms, nil)

		err := app.NewDefaultApp(context.Background())
		if !assert.NoError(t, err) {
			return
		}
		s, err := New(
			withMockProvider(mp),
			WithNumberOfStreams(1),
			WithShutdownPriority(app.ShutdownPriority(10)),
		)
		assert.NotNil(t, s)
		assert.NoError(t, err)

		err = app.Shutdown(context.Background())
		assert.NoError(t, err)

		ms.AssertExpectations(t)
		mp.AssertExpectations(t)
	})
}

func TestMustNew(t *testing.T) {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	assert.Panics(t, func() {
		MustNew()
	})

	assert.NotPanics(t, func() {
		p := &mockProvider{}
		p.On("MakeStream").Return(&mockStream{}, nil)

		MustNew(
			withMockProvider(p),
			WithNumberOfStreams(1),
		)
	})
}
