package implqueue

import (
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/arquivei/goduck"
	"github.com/arquivei/goduck/impl/implqueue/pubsubqueue"
)

// PubsubConfigs contains the configs for pubsub connection
type PubsubConfigs struct {
	ProjectID    string
	Subscription string
}

type pubsubConsumer struct {
	client       *pubsub.Client
	subscription *pubsub.Subscription
	nextMessage  chan *pubsub.Message
	errChannel   chan error
	closeOnce    *sync.Once
	cancelFn     func()
}

// NewPubsubQueue creates a new pubsub queue
//
// Deprecated: use pubsubqueue.New instead
func NewPubsubQueue(config PubsubConfigs) (goduck.MessagePool, error) {
	return pubsubqueue.New(pubsubqueue.PubsubConfigs{
		ProjectID:    config.ProjectID,
		Subscription: config.Subscription,
	})
}
