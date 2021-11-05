package inputstreams

import (
	"context"
	"strconv"

	"github.com/arquivei/foundationkit/app"
	"github.com/arquivei/goduck"
	"github.com/arquivei/goduck/middleware/streammiddleware"
)

// New returns a new slice of goduck streams.
// By default it returns 1 stream. This can be configured by passing WithNumberOfStreams option.
// The new streams uses the default foundationkit app. This can be configured by passing WithApp option.
// The shudown priority defaults to zero. This can be configured by passing WithShutdownPriority option.
func New(opts ...Option) ([]goduck.Stream, error) {
	o := options{
		streams: 1,
	}

	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}

	if o.provider == nil {
		return nil, ErrNoProvider
	}

	if o.streams < 1 {
		return nil, ErrZeroStreams
	}

	goduckStreams := make([]goduck.Stream, o.streams)
	var err error
	for i := range goduckStreams {
		name := "goduck_stream_" + strconv.Itoa(i)
		if goduckStreams[i], err = makeSingleStream(name, o); err != nil {
			return nil, err
		}
	}
	return goduckStreams, nil
}

func makeSingleStream(name string, o options) (goduck.Stream, error) {
	stream, err := o.provider.MakeStream()
	if err != nil {
		return nil, err
	}
	stream = streammiddleware.WrapWithLogging(stream)
	closeFunc := stream.Close

	shutdownHandler := &app.ShutdownHandler{
		Name:     name,
		Handler:  func(_ context.Context) error { return closeFunc() },
		Priority: o.app.shutdownPriority,
	}

	if o.app.app != nil {
		o.app.app.RegisterShutdownHandler(shutdownHandler)
	} else {
		app.RegisterShutdownHandler(shutdownHandler)
	}

	return stream, nil
}

// MustNew calls New but panics in case of error.
func MustNew(opts ...Option) []goduck.Stream {
	s, err := New(opts...)
	if err != nil {
		panic(err)
	}
	return s
}
