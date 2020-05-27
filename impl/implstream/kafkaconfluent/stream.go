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

type goduckStream struct {
	consumer *kafka.Consumer

	fetchRequestChan chan struct{}
	msgChan          chan *kafka.Message

	unackedMessages     map[topicPartition]kafka.Offset
	unackedMessagesLock *sync.Mutex

	done      chan struct{}
	waitGroup *sync.WaitGroup

	timeout time.Duration
}

// MustNew creates a confluent-kafka-go goduck.Stream with default configs
func MustNew(
	brokers []string,
	topics []string,
	groupID string,
	username string,
	password string,
) goduck.Stream {
	config := &kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(brokers, ","),
		"group.id":                 groupID,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": "false",
		"security.protocol":        "sasl_plaintext",
		"sasl.mechanisms":          "PLAIN",
		"sasl.username":            username,
		"sasl.password":            password,
	}
	return MustNewWithCustomConfigs(topics, config)
}

// MustNewWithCustomConfigs creates a confluent-kafka-go goduck.Stream with custom configs
func MustNewWithCustomConfigs(topics []string, configs *kafka.ConfigMap) goduck.Stream {
	c, err := kafka.NewConsumer(configs)
	if err != nil {
		panic(err)
	}

	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		panic(err)
	}

	stream := &goduckStream{
		consumer:            c,
		fetchRequestChan:    make(chan struct{}),
		msgChan:             make(chan *kafka.Message),
		unackedMessages:     make(map[topicPartition]kafka.Offset),
		unackedMessagesLock: &sync.Mutex{},
		done:                make(chan struct{}),
		waitGroup:           &sync.WaitGroup{},
		timeout:             time.Second,
	}

	stream.waitGroup.Add(1)
	go stream.backgroundPoll()

	return stream
}

func (c *goduckStream) Next(ctx context.Context) (goduck.RawMessage, error) {
	msg, err := c.readWithContext(ctx)
	if err != nil {
		return nil, err
	}
	c.markUnackedMessage(msg)

	return goduckMsg(msg.Value), nil
}

func (c *goduckStream) readWithContext(ctx context.Context) (*kafka.Message, error) {
	// request other goroutine to poll
	select {
	case c.fetchRequestChan <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, io.EOF
	}

	// wait for poll response
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg, ok := <-c.msgChan:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	}
}

func (c *goduckStream) backgroundPoll() {
	defer c.waitGroup.Done()

	for {
		// wait for request
		select {
		case <-c.done:
			break
		case <-c.fetchRequestChan:
		}

		// poll
		msg, err := c.pollNextMessage()

		if err != nil {
			break
		}
		// return response
		select {
		case c.msgChan <- msg:
		case <-c.done:
			break
		}

	}

	close(c.msgChan)
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
	close(c.done)
	c.waitGroup.Wait()
	c.consumer.Close()
	return nil
}
