package kafkasarama

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/arquivei/foundationkit/errors"
	"github.com/rs/zerolog/log"

	"github.com/arquivei/goduck"
)

// KafkaConfigs contains the configs for kafka connection
type KafkaConfigs struct {
	Brokers []string
	Topics  []string
	GroupID string

	Username string
	Password string
}

type goduckStream struct {
	consumer sarama.ConsumerGroup
	topics   []string
	handler  *consumerGroupHandler
	cancelFn func()
}

func MustNewKafkaStream(config KafkaConfigs) goduck.Stream {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Net.SASL.Enable = true
	saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	saramaConfig.Consumer.IsolationLevel = sarama.ReadCommitted
	saramaConfig.Net.SASL.User = config.Username
	saramaConfig.Net.SASL.Password = config.Password
	saramaConfig.Version = sarama.V2_3_0_0
	return MustNewKafkaStreamWithSaramaConfigs(config, saramaConfig)
}

func MustNewKafkaStreamWithSaramaConfigs(config KafkaConfigs, saramaConfig *sarama.Config) goduck.Stream {
	const op = errors.Op("kafkasarama.MustNewKafkaStreamWithSaramaConfigs")
	consumer, err := sarama.NewConsumerGroup(config.Brokers, config.GroupID, saramaConfig)
	if err != nil {
		panic(errors.E(op, err))
	}
	handler := newHandler()
	stream := &goduckStream{
		consumer: consumer,
		handler:  handler,
		topics:   config.Topics,
	}
	go stream.run()
	return stream
}

func (c *goduckStream) run() {
	ctx := context.Background()
	ctx, c.cancelFn = context.WithCancel(ctx)
	defer c.cancelFn()

	for ctx.Err() == nil {
		err := c.consumer.Consume(ctx, c.topics, c.handler)
		if err != nil {
			log.Error().Err(err).Msg("Error consuming messages")
		}
	}
	c.Close()
}

func (c *goduckStream) Next(ctx context.Context) (goduck.RawMessage, error) {
	const op = errors.Op("kafkasarama.goduckStream.Next")
	msg, err := c.handler.Next(ctx)
	if err != nil {
		return nil, errors.E(op, err)
	}
	return rawMessage{
		bytes:    msg.Value,
		metadata: getMetadataFromMessage(msg),
	}, nil
}
func (c *goduckStream) Done(ctx context.Context) error {
	c.handler.Done()
	return nil
}

func (c *goduckStream) Close() error {
	// makes main loop stop
	c.cancelFn()

	// closes message channel, and Next() calls return io.EOF from now on
	c.handler.Close()
	return nil
}

func getMetadataFromMessage(msg *sarama.ConsumerMessage) map[string][]byte {
	meta := map[string][]byte{}
	for _, header := range msg.Headers {
		meta[string(header.Key)] = header.Value
	}
	meta[goduck.KeyMetadata] = msg.Key
	return meta
}
