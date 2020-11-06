package kafkasegmentio

import (
	"context"
	"time"

	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck"

	"github.com/segmentio/kafka-go"
	kafkaGo "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// Configs contains the configs for kafka connection
type Configs struct {
	Brokers  []string
	Topic    string
	GroupID  string
	Username string
	Password string

	// SegmentIO configs

	CommitInterval time.Duration
}

type kafkaConsumer struct {
	reader             *kafkaGo.Reader
	uncommitedMessages []kafkaGo.Message
}

// New creates a new goduck.Stream from kafka, using segmentio library
func New(config Configs) goduck.Stream {
	reader := kafkaGo.NewReader(
		kafkaGo.ReaderConfig{
			GroupID:        config.GroupID,
			Brokers:        config.Brokers,
			Topic:          config.Topic,
			CommitInterval: config.CommitInterval,
			Dialer: &kafka.Dialer{
				Timeout: 10 * time.Second,
				SASLMechanism: plain.Mechanism{
					Username: config.Username,
					Password: config.Password,
				},
			},
		},
	)
	return &kafkaConsumer{
		reader: reader,
	}
}

// NewWithCustomConfigs allows creating a kafka connection with all
// configs from the SegmentIO library
func NewWithCustomConfigs(config kafkaGo.ReaderConfig) goduck.Stream {
	reader := kafkaGo.NewReader(config)
	return &kafkaConsumer{
		reader: reader,
	}
}

func (c *kafkaConsumer) Next(ctx context.Context) (goduck.RawMessage, error) {
	const op = errors.Op("kafkaConsumer.Poll")
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return nil, errors.E(op, err)
	}
	c.uncommitedMessages = append(c.uncommitedMessages, msg)
	result := rawMessage{
		bytes:    msg.Value,
		metadata: getMetadataFromMessage(msg),
	}
	return result, nil
}
func (c *kafkaConsumer) Done(ctx context.Context) error {
	const op = errors.Op("kafkaConsumer.Done")
	err := c.reader.CommitMessages(ctx, c.uncommitedMessages...)
	if err != nil {
		return errors.E(op, err)
	}
	c.uncommitedMessages = c.uncommitedMessages[:0]
	return nil
}

func (c *kafkaConsumer) Close() error {
	const op = errors.Op("kafkaConsumer.Close")
	err := c.reader.Close()
	return errors.E(op, err)
}

func getMetadataFromMessage(msg kafka.Message) map[string][]byte {
	meta := map[string][]byte{}
	for _, header := range msg.Headers {
		meta[header.Key] = header.Value
	}
	meta[goduck.KeyMetadata] = msg.Key
	return meta
}
