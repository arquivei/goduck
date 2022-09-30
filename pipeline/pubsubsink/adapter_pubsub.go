package pubsubsink

import (
	"context"

	"cloud.google.com/go/pubsub"
)

type clientAdapter struct {
	*pubsub.Client
}

func NewPubsubClientAdapter(client *pubsub.Client) clientAdapter {
	return clientAdapter{client}
}

// NewClientAdapter creates a new PubsubClient from a pubsub.Client
func (c *clientAdapter) Topic(id string) TopicGateway {
	return newPubSubTopicAdapter(c.Client.Topic(id))
}

type topicAdapter struct {
	*pubsub.Topic
}

// NewTopicAdapter creates a new TopicGateway from a pubsub.Topic
func newPubSubTopicAdapter(topic *pubsub.Topic) TopicGateway {
	return &topicAdapter{topic}
}

// Publish publishes a message to the topic
func (t *topicAdapter) Publish(ctx context.Context, msg *pubsub.Message) PublishResult {
	return t.Topic.Publish(ctx, msg)
}
