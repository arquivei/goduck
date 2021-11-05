package inputstreams

import "errors"

var (
	// ErrZeroStreams is returned if the chosen number of streams is less than one.
	ErrZeroStreams = errors.New("invalid number of streams, must be higher than zero")
	// ErrNoProvider is returned if no provider is set.
	ErrNoProvider = errors.New("no provider configured")
	// ErrConflictionAppOptions is returned with inputstream is provided with conflicting app configuration, like
	// setting a specific app.App but also using the defualt app.
	ErrConflictionAppOptions = errors.New("confliction app options detected")
)
