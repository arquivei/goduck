package pubsubsink

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck/pipeline"
)

// PublishResult is the result of a Publish call for the TopicGateway.
type PublishResult interface {
	Get(ctx context.Context) (string, error)
}

// TopicGateway represents the gateway to a pubsub topic.
type TopicGateway interface {
	Publish(ctx context.Context, msg *pubsub.Message) PublishResult
	Exists(ctx context.Context) (bool, error)
	Stop()
}

// Sink is a pubsub sink to a specific topic
type Sink struct {
	topicGateway TopicGateway
}

// SinkMessage is a message to be sent to a pubsub topic
type SinkMessage struct {
	Msg []byte
}

// MustNew creates a new pubsub sink or panics if fails
func MustNew(topicGateway TopicGateway) (sink *Sink, closeFunc func()) {
	op := errors.Op("pubsubsink.MustNew")
	fmt.Println(topicGateway == nil)
	if topicGateway == nil {
		panic(errors.E(op, "topic gateway is nil"))
	}

	ok, err := topicGateway.Exists(context.Background())
	if err != nil {
		panic(errors.E(op, err))
	}
	if !ok {
		panic(errors.E(op, "topic does not exist"))
	}

	closeFunc = func() {
		topicGateway.Stop()
	}

	return &Sink{
		topicGateway: topicGateway,
	}, closeFunc
}

// Store stores messages in a pubsub topic. It returns an error if any of the messages return an error.
func (s *Sink) Store(ctx context.Context, messages ...pipeline.SinkMessage) error {
	op := errors.Op("pubsubsink.Sink.Store")
	for _, msg := range messages {
		sinkMsg, ok := msg.(SinkMessage)
		if !ok {
			return errors.E(op, "invalid message type: expected pubsubsink.SinkMessage")
		}
		_, err := s.topicGateway.Publish(ctx, &pubsub.Message{
			Data: sinkMsg.Msg,
		}).Get(ctx)
		if err != nil {
			return errors.E(op, err)
		}
	}
	return nil
}
