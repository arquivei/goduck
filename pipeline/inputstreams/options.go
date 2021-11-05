package inputstreams

import "github.com/arquivei/foundationkit/app"

type options struct {
	provider Provider
	streams  int
	app      appOptions
}

type appOptions struct {
	app              *app.App // Will use the default app if this is nil
	shutdownPriority app.ShutdownPriority
}

// Option allows for configuring the input stream.
type Option func(*options) error

// WithNumberOfStreams sets the number of streams returned by New.
// Defaults to 1 stream if not provided.
func WithNumberOfStreams(n int) Option {
	return func(o *options) error {
		// This will be validated later
		o.streams = n
		return nil
	}
}

// WithShutdownPriority sets the shutdown priority when configuring the graceful shudown for the input streams.
// If not provided, the default is zero, the lowest priority.
func WithShutdownPriority(p app.ShutdownPriority) Option {
	return func(o *options) error {
		o.app.shutdownPriority = p
		return nil
	}
}

// WithApp tells the pipeline that we are using a foundationkit's app so it
// may register the shutdown handler for the streams it creates on the given app.
// This will override the default which is to use de default app.
func WithApp(a *app.App) Option {
	return func(o *options) error {
		o.app.app = a
		return nil
	}
}
