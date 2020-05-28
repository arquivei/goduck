package kafkaconfluent

import (
	"context"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck"
	"github.com/rs/zerolog/log"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

	// forces copying the librdkafka folder to vendor/
	_ "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka/librdkafka"
)

// Config contains the configuration necessary to build the
// confluent-kafka-go goduck.Stream
type Config struct {
	Brokers  []string
	GroupID  string
	Username string
	Password string

	// rdKafkaConfig can specify librdkafka configs. If this is variable is set, the
	// other variables (Brokers, GroupID, Username and Password) are ignored
	rdKafkaConfig *kafka.ConfigMap

	Topics []string

	// PoolTimeout is the value passed to the internal consumer.Pool(...)
	// function. Default: 1s
	PoolTimeout time.Duration
}

type goduckStream struct {
	consumer *kafka.Consumer

	controller *requestController

	unackedMessages     map[topicPartition]kafka.Offset
	unackedMessagesLock *sync.Mutex

	done      chan struct{}
	waitGroup *sync.WaitGroup

	timeout time.Duration
}

// MustNew creates a confluent-kafka-go goduck.Stream with default configs
func MustNew(config Config) goduck.Stream {
	if config.rdKafkaConfig == nil {
		if config.Brokers == nil || len(config.Brokers) == 0 {
			panic("Empty broker")
		}

		if config.Username == "" {
			panic("Empty username")
		}

		if config.Password == "" {
			panic("Empty password")
		}

		config.rdKafkaConfig = &kafka.ConfigMap{
			"bootstrap.servers":        strings.Join(config.Brokers, ","),
			"group.id":                 config.GroupID,
			"auto.offset.reset":        "earliest",
			"enable.auto.offset.store": "false",
			"security.protocol":        "sasl_plaintext",
			"sasl.mechanisms":          "PLAIN",
			"sasl.username":            config.Username,
			"sasl.password":            config.Password,
		}
	}

	if config.Topics == nil || len(config.Topics) == 0 {
		panic("Empty broker")
	}
	if config.PoolTimeout == 0 {
		config.PoolTimeout = time.Second
	}
	return mustCreateStream(config)
}

func mustCreateStream(config Config) goduck.Stream {
	c, err := kafka.NewConsumer(config.rdKafkaConfig)
	if err != nil {
		panic(err)
	}

	err = c.SubscribeTopics(config.Topics, nil)
	if err != nil {
		panic(err)
	}

	stream := &goduckStream{
		consumer:            c,
		controller:          newRequestController(),
		unackedMessages:     make(map[topicPartition]kafka.Offset),
		unackedMessagesLock: &sync.Mutex{},
		done:                make(chan struct{}),
		waitGroup:           &sync.WaitGroup{},
		timeout:             config.PoolTimeout,
	}

	stream.waitGroup.Add(1)
	go stream.backgroundPoll()

	return stream
}

func (c *goduckStream) Next(ctx context.Context) (goduck.RawMessage, error) {
	err := c.controller.requestJob()
	if err != nil {
		return nil, err
	}

	msg, err := c.controller.getResult(ctx)
	if err != nil {
		return nil, err
	}

	c.markUnackedMessage(msg)

	return goduckMsg(msg.Value), nil
}

func (c *goduckStream) backgroundPoll() {
	defer c.waitGroup.Done()

	for {
		// wait for request
		err := c.controller.getNextJob()
		if err != nil {
			break
		}

		// poll from kafka (blocking)
		msg, err := c.pollNextMessage()
		if err != nil {
			break
		}

		// return response
		err = c.controller.submitResult(msg)
		if err != nil {
			break
		}
	}
}

func (c *goduckStream) pollNextMessage() (*kafka.Message, error) {
	const op = errors.Op("pollNextMessage")
	for {
		select {
		case <-c.done:
			return nil, io.EOF
		default:
		}

		msg, err := c.consumer.ReadMessage(c.timeout)
		if err != nil && err.(kafka.Error).Code() == kafka.ErrTimedOut {
			continue
		}
		if err != nil {
			err = errors.E(op, err)
			log.Error().Err(err).Msg("failed to poll new kafka messages")
			return nil, err
		}
		return msg, nil

	}
}

func (c *goduckStream) markUnackedMessage(msg *kafka.Message) {
	c.unackedMessagesLock.Lock()
	defer c.unackedMessagesLock.Unlock()
	tp := topicPartition{msg.TopicPartition.Topic, msg.TopicPartition.Partition}
	c.unackedMessages[tp] = msg.TopicPartition.Offset
}

func (c *goduckStream) Done(ctx context.Context) error {
	const op = errors.Op("kafkaconfluent.goduckStream.Done")

	c.unackedMessagesLock.Lock()
	defer c.unackedMessagesLock.Unlock()

	offsets := make(kafka.TopicPartitions, 0, len(c.unackedMessages))
	for tp, offset := range c.unackedMessages {
		kafkaTp := kafka.TopicPartition{
			Topic:     tp.topic,
			Partition: tp.partition,
			Offset:    offset,
		}
		offsets = append(offsets, kafkaTp)
	}

	_, err := c.consumer.CommitOffsets(offsets)
	if err != nil {
		errors.E(op, err)
	}

	c.unackedMessages = make(map[topicPartition]kafka.Offset)
	return nil
}

func (c *goduckStream) Close() error {
	c.controller.close()
	close(c.done)

	c.waitGroup.Wait()
	c.consumer.Close()
	return nil
}
