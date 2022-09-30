package pubsubsink

import (
	"context"

	"cloud.google.com/go/pubsub"
)

type clientAdapter struct {
	*pubsub.Client
}

// NewPubSubClientAdapter creates a new PubsubClientGateway from a pubsub.Client
func NewPubsubClientAdapter(client *pubsub.Client) PubsubClientGateway {
	return clientAdapter{client}
}

// NewClientAdapter creates a new PubsubClient from a pubsub.Client
func (c clientAdapter) Topic(id string) topicGateway {
	return newPubSubTopicAdapter(c.Client.Topic(id))
}

type topicAdapter struct {
	*pubsub.Topic
}

// newTopicAdapter creates a new TopicGateway from a pubsub.Topic
func newPubSubTopicAdapter(topic *pubsub.Topic) topicGateway {
	return &topicAdapter{topic}
}

// Publish publishes a message to the topic
func (t *topicAdapter) Publish(ctx context.Context, msg *pubsub.Message) publishResult {
	return t.Topic.Publish(ctx, msg)
}
