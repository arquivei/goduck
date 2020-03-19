package implstream

import (
	"context"
	"time"

	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck"
	"github.com/segmentio/kafka-go"
	kafkaGo "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// KafkaConfigs contains the configs for kafka connection
type KafkaConfigs struct {
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

func NewKafkaStream(config KafkaConfigs) goduck.Stream {
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

// NewKafkaStreamWithCustomConfigs allows creating a kafka connection with all
// configs from the SegmentIO library
func NewKafkaStreamWithCustomConfigs(config kafkaGo.ReaderConfig) goduck.Stream {
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
	result := rawMessage(msg.Value)
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
