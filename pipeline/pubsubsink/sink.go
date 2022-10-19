package pubsubsink

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck/pipeline"
)

// publishResult is the result of a Publish call for the TopicGateway.
type publishResult interface {
	Get(ctx context.Context) (string, error)
}

// topicGateway represents the gateway to a pubsub topic.
type topicGateway interface {
	Publish(ctx context.Context, msg *pubsub.Message) publishResult
	Exists(ctx context.Context) (bool, error)
	Stop()
}

// PubSubClientGateway represents the gateway to a pubsub client.
type PubsubClientGateway interface {
	Topic(name string) topicGateway
	Close() error
}

// Sink is a pubsub sink
type Sink struct {
	pubsubClient PubsubClientGateway
}

// SinkMessage is a message to be sent to a pubsub topic
type SinkMessage struct {
	Topic string
	Msg   []byte
}

// MustNew creates a new pubsub sink or panics if fails
func MustNew(client PubsubClientGateway) (sink *Sink, closeFunc func()) {
	op := errors.Op("pubsubsink.MustNew")
	if client == nil {
		panic(errors.E(op, "topic gateway is nil"))
	}

	closeFunc = func() {
		client.Close()
	}

	return &Sink{
		pubsubClient: client,
	}, closeFunc
}

func (s *Sink) getTopic(ctx context.Context, topicID string) (topic topicGateway, closeFunc func(), err error) {
	op := errors.Op("pubsubsink.sink.getTopic")
	topic = s.pubsubClient.Topic(topicID)
	ok, err := topic.Exists(ctx)
	if err != nil {
		return nil, nil, errors.E(op, err)
	}
	if !ok {
		return nil, nil, errors.E(op, "topic does not exist", errors.KV("topic", topicID))
	}
	return topic, topic.Stop, nil
}

// Store stores messages in a pubsub topic. It returns an error if any of the messages return an error.
func (s *Sink) Store(ctx context.Context, messages ...pipeline.SinkMessage) error {
	op := errors.Op("pubsubsink.Sink.Store")

	topicStorage := make(map[string]topicGateway)
	closeTopics := []func(){}
	defer func() {
		for _, closeTopic := range closeTopics {
			closeTopic()
		}
	}()

	for _, msg := range messages {
		sinkMsg, ok := msg.(SinkMessage)
		if !ok {
			return errors.E(op, "invalid message type: expected pubsubsink.SinkMessage")
		}
		if sinkMsg.Topic == "" {
			return errors.E(op, "topic is empty")
		}

		topic, ok := topicStorage[sinkMsg.Topic]
		if !ok {
			var closeFunc func()
			var err error
			topic, closeFunc, err = s.getTopic(ctx, sinkMsg.Topic)
			if err != nil {
				return errors.E(op, err)
			}
			topicStorage[sinkMsg.Topic] = topic
			closeTopics = append(closeTopics, closeFunc)
		}

		_, err := topic.Publish(ctx, &pubsub.Message{
			Data: sinkMsg.Msg,
		}).Get(ctx)
		if err != nil {
			return errors.E(op, err)
		}
	}
	return nil
}
