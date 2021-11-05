package inputstreams

import "github.com/arquivei/goduck"

// Provider is an interface for creating goduck streams.
type Provider interface {
	MakeStream() (goduck.Stream, error)
}
