package kafkasink

import (
	"context"

	"github.com/arquivei/goduck/pipeline"

	"github.com/arquivei/foundationkit/errors"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type kafkaPusher struct {
	producer *kafka.Producer
}

// SinkMessage is the input for the Kafka Sink
type SinkMessage struct {
	Topic string
	Key   []byte
	Value []byte
}

// MustNew creates a new pipeline sink that saves messages to kafka
func MustNew(brokers, username, password string) (pipeline.Sink, func()) {
	return MustNewWithAuth(brokers, username, password, "", "sasl_plaintext")
}

// MustNewWithAuth creates a new pipeline sink that saves messages to kafka
func MustNewWithAuth(brokers, username, password, certPath, securityProtocol string) (pipeline.Sink, func()) {
	if brokers == "" {
		panic("missing kafka brokers")
	}

	if username == "" {
		panic("missing kafka username")
	}

	if password == "" {
		panic("missing kafka password")
	}

	if securityProtocol == "sasl_ssl" && certPath == "" {
		panic("missing kafka cert path")
	}

	configs := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"compression.codec": "gzip",
		"partitioner":       "murmur2_random",
		"sasl.mechanisms":   "PLAIN",
		"sasl.password":     password,
		"sasl.username":     username,
		"security.protocol": securityProtocol,
		"ssl.ca.location":   certPath,
	}

	producer, err := kafka.NewProducer(configs)
	if err != nil {
		panic(err)
	}

	pusher := &kafkaPusher{
		producer: producer,
	}

	closeFn := func() {
		producer.Close()
	}

	return pusher, closeFn
}

func (p *kafkaPusher) Store(ctx context.Context, messages ...pipeline.SinkMessage) error {
	const op = errors.Op("kafkasink.kafkaPusher.Store")

	sdkMessages := make([]*kafka.Message, len(messages))
	for i, m := range messages {
		message := m.(SinkMessage)
		sdkMsg := &kafka.Message{
			Key:   message.Key,
			Value: message.Value,
			TopicPartition: kafka.TopicPartition{
				Topic:     &message.Topic,
				Partition: kafka.PartitionAny,
			},
		}
		sdkMessages[i] = sdkMsg
	}

	deliveryChan := make(chan kafka.Event, len(sdkMessages))

	for _, msg := range sdkMessages {
		err := p.producer.Produce(msg, deliveryChan)
		if err != nil {
			return errors.E(op, err, errors.SeverityRuntime)
		}
	}
	for i := 0; i < len(sdkMessages); i++ {
		e := <-deliveryChan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			return errors.E(op, m.TopicPartition.Error, errors.SeverityRuntime)
		}
	}

	return nil
}
