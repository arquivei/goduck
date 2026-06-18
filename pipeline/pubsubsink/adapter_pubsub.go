package pubsubsink

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub/v2"
	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type clientAdapter struct {
	*pubsub.Client
}

// NewPubsubClientAdapter creates a new PubsubClientGateway from a pubsub.Client
func NewPubsubClientAdapter(client *pubsub.Client) PubsubClientGateway {
	return clientAdapter{client}
}

func (c clientAdapter) Topic(id string) topicGateway {
	return &topicAdapter{
		publisher:   c.Client.Publisher(id),
		adminClient: c.Client,
		topicID:     fmt.Sprintf("projects/%s/topics/%s", c.Client.Project(), id),
	}
}

func (c clientAdapter) Close() error {
	return c.Client.Close()
}

type topicAdapter struct {
	publisher   *pubsub.Publisher
	adminClient *pubsub.Client
	topicID     string
}

func (t *topicAdapter) Publish(ctx context.Context, msg *pubsub.Message) publishResult {
	return t.publisher.Publish(ctx, msg)
}

func (t *topicAdapter) Exists(ctx context.Context) (bool, error) {
	_, err := t.adminClient.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{
		Topic: t.topicID,
	})

	if status.Code(err) == codes.NotFound {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func (t *topicAdapter) Stop() {
	t.publisher.Stop()
}
