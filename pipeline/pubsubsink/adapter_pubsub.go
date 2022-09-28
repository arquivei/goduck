package pubsubsink

import (
	"context"

	"cloud.google.com/go/pubsub"
)

type topicAdapter struct {
	*pubsub.Topic
}

// NewTopicAdapter creates a new TopicGateway from a pubsub.Topic
func NewPubSubTopicAdapter(topic *pubsub.Topic) TopicGateway {
	return &topicAdapter{topic}
}

// Publish publishes a message to the topic
func (t *topicAdapter) Publish(ctx context.Context, msg *pubsub.Message) PublishResult {
	return t.Topic.Publish(ctx, msg)
}
