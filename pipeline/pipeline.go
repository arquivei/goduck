package pipeline

import (
	"context"

	"github.com/arquivei/foundationkit/app"
	"github.com/arquivei/foundationkit/errors"
)

// TODO: These could be the default value, but pipeline should allow for configuring these values.
const (
	// ShutdownPriorityEngine opinionated value for the engine shutdown
	// priority. Change this if your app needs another value
	ShutdownPriorityEngine = app.ShutdownPriority(100)
	// ShutdownPriorityStream opinionated value for the input stream shutdown
	// priority. Change this if your app needs another value
	ShutdownPriorityStream = app.ShutdownPriority(70)
)

// Pipeline is a goduck worker that read messages, proccess them and send them to a sink.
type Pipeline interface {
	Run(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

type pipeline struct {
	engine   goDuckEngine
	shutdown func()
	done     chan struct{}
	err      error
}

// goDuckEngine is a basic interface for abstracting the goduck engine underneath.
type goDuckEngine interface {
	Run(ctx context.Context) error
}

// MustNew returns a new pipeline but panics in case of error.
func MustNew(opts ...Option) Pipeline {
	pipeline, err := New(opts...)
	if err != nil {
		panic(err)
	}
	return pipeline
}

// New returns a new pipeline.
func New(opts ...Option) (Pipeline, error) {
	const op = errors.Op("pipeline.New")

	var builderOpts pipelineBuilderOptions
	for _, opt := range opts {
		opt(&builderOpts)
	}

	pipeline, err := build(builderOpts)
	if err != nil {
		return nil, errors.E(op, err)
	}

	app.RegisterShutdownHandler(&app.ShutdownHandler{
		Name:     "pipeline",
		Handler:  pipeline.Shutdown,
		Priority: ShutdownPriorityEngine,
	})
	return pipeline, nil
}
func (p *pipeline) Run(ctx context.Context) error {
	defer close(p.done)
	ctx, p.shutdown = context.WithCancel(ctx)
	p.err = p.engine.Run(ctx)
	return p.err
}

func (p *pipeline) Shutdown(ctx context.Context) error {
	p.shutdown()
	select {
	case <-ctx.Done():
		return ErrShutdown
	case <-p.done:
		return p.err
	}
}
