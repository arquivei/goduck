package implqueue

import (
	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck"
	"github.com/arquivei/goduck/impl/implqueue/pubsubqueue"
)

// PubsubConfigs contains the configs for pubsub connection
type PubsubConfigs struct {
	ProjectID    string
	Subscription string
}

// NewPubsubQueue creates a new goduck.MessagePool that reads from pubsub.
// Deprecated: use pubsubqueue.New instead
func NewPubsubQueue(config PubsubConfigs) (goduck.MessagePool, error) {
	const op = errors.Op("implpubsub.NewPubsubQueue")
	consumer, err := pubsubqueue.New(pubsubqueue.PubsubConfigs{
		ProjectID:    config.ProjectID,
		Subscription: config.Subscription,
	})
	return consumer, errors.E(op, err)
}
