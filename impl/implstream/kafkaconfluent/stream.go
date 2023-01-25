package kafkaconfluent

import (
	"context"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/arquivei/goduck"

	"github.com/arquivei/foundationkit/errors"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
)

var (
	//ErrEmptyBroker is returned when the the broker is missing from the Config struct
	ErrEmptyBroker = errors.New("bad config: empty broker")
	//ErrEmptyTopic is returned when the the Topic is missing from the Config struct
	ErrEmptyTopic = errors.New("bad config: empty topic")
	//ErrEmptyUsername is returned when the the Username is missing from the Config struct
	ErrEmptyUsername = errors.New("bad config: empty username")
	//ErrEmptyPassword is returned when the the Password is missing from the Config struct
	ErrEmptyPassword = errors.New("bad config: empty password")
)

// Config contains the configuration necessary to build the
// confluent-kafka-go goduck.Stream
type Config struct {
	Brokers  []string
	GroupID  string
	Username string
	Password string

	// RDKafkaConfig can specify librdkafka configs. If this is variable is set, the
	// other variables (Brokers, GroupID, Username and Password) are ignored
	RDKafkaConfig *kafka.ConfigMap

	Topics []string

	// PoolTimeout is the value passed to the internal consumer.Pool(...)
	// function. Default: 1s
	PoolTimeout time.Duration

	// DisableCommit indicates that offsets should never be commited, even
	// after calling Done()
	DisableCommit bool
}

type goduckStream struct {
	consumer *kafka.Consumer

	controller *requestController

	unackedMessages     map[topicPartition]kafka.Offset
	unackedMessagesLock *sync.Mutex

	done      chan struct{}
	waitGroup *sync.WaitGroup

	timeout       time.Duration
	disableCommit bool
}

// New creates a confluent-kafka-go goduck.Stream with default configs
func New(config Config) (goduck.Stream, error) {
	if len(config.Topics) == 0 {
		return nil, ErrEmptyTopic
	}

	if config.RDKafkaConfig == nil {
		if len(config.Brokers) == 0 {
			return nil, ErrEmptyBroker
		}

		if config.Username == "" {
			return nil, ErrEmptyUsername
		}

		if config.Password == "" {
			return nil, ErrEmptyPassword
		}

		config.RDKafkaConfig = &kafka.ConfigMap{
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

	if config.PoolTimeout == 0 {
		config.PoolTimeout = time.Second
	}
	return createStream(config)
}

// MustNew creates a confluent-kafkam with default configs
func MustNew(config Config) goduck.Stream {
	s, err := New(config)
	if err != nil {
		panic(err)
	}
	return s
}

func createStream(config Config) (goduck.Stream, error) {
	c, err := kafka.NewConsumer(config.RDKafkaConfig)
	if err != nil {
		return nil, err
	}

	err = c.SubscribeTopics(config.Topics, nil)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	stream := &goduckStream{
		consumer:            c,
		controller:          newRequestController(done),
		unackedMessages:     make(map[topicPartition]kafka.Offset),
		unackedMessagesLock: &sync.Mutex{},
		done:                done,
		waitGroup:           &sync.WaitGroup{},
		timeout:             config.PoolTimeout,
		disableCommit:       config.DisableCommit,
	}

	stream.waitGroup.Add(1)
	go stream.backgroundPoll()

	return stream, nil
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
	if c.disableCommit {
		return
	}
	c.unackedMessagesLock.Lock()
	defer c.unackedMessagesLock.Unlock()
	tp := topicPartition{msg.TopicPartition.Topic, msg.TopicPartition.Partition}
	c.unackedMessages[tp] = msg.TopicPartition.Offset
}

func (c *goduckStream) Done(ctx context.Context) error {
	const op = errors.Op("kafkaconfluent.goduckStream.Done")

	if c.disableCommit {
		return nil
	}

	c.unackedMessagesLock.Lock()
	defer c.unackedMessagesLock.Unlock()

	offsets := make(kafka.TopicPartitions, 0, len(c.unackedMessages))
	for tp, offset := range c.unackedMessages {
		kafkaTp := kafka.TopicPartition{
			Topic:     tp.topic,
			Partition: tp.partition,
			Offset:    offset + 1,
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
	// from now on, no further attempts to pool new messages will be made.
	// if there is an in-flight ReadMessage() call, it will be finished, but
	// its result won't be delivered
	close(c.done)

	c.waitGroup.Wait()
	c.consumer.Close()
	return nil
}
