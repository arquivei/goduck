package dlqmiddleware

import (
	"context"
	"strings"
	"sync"

	"github.com/arquivei/foundationkit/app"
	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
)

type dlqMiddleware struct {
	nextBatch     goduck.BatchProcessor
	nextSingle    goduck.Processor
	topic         string
	kafkaProducer *kafka.Producer
	isNoop        bool
}

// WrapBatch wraps @next with a middleware that redirect any failed messages
// to a dlq. Fatal errors and parent context cancelation errors are ignored.
func WrapBatch(
	next goduck.BatchProcessor,
	brokers []string,
	topic, username, password string,
	isNoop bool,
) goduck.BatchProcessor {
	return wrap(next, nil, brokers, topic, username, password, isNoop)
}

// WrapSingle wraps @next with a middleware that redirect any failed messages
// to a dlq. Fatal errors and parent context cancelation errors are ignored.
func WrapSingle(
	next goduck.Processor,
	brokers []string,
	topic, username, password string,
	isNoop bool,
) goduck.Processor {
	return wrap(nil, next, brokers, topic, username, password, isNoop)
}

func wrap(
	nextBatch goduck.BatchProcessor,
	nextSingle goduck.Processor,
	brokers []string,
	topic, username, password string,
	isNoop bool,
) goduck.AnyProcessor {
	var err error
	var kafkaProducer *kafka.Producer

	if !isNoop {
		checkProcessorParams(brokers, topic, username, password)

		kafkaProducer, err = kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": strings.Join(brokers, ","),
			"security.protocol": "sasl_plaintext",
			"sasl.mechanisms":   "PLAIN",
			"sasl.username":     username,
			"sasl.password":     password,
			"compression.codec": "gzip",
		})
		if err != nil {
			panic(err)
		}

		// TODO: This should be configured.
		app.RegisterShutdownHandler(&app.ShutdownHandler{
			Name: "goduck_middleware_kafka_dlq",
			Handler: func(ctx context.Context) error {
				kafkaProducer.Close()
				return nil
			},
		})
	}

	return dlqMiddleware{
		nextBatch:     nextBatch,
		nextSingle:    nextSingle,
		kafkaProducer: kafkaProducer,
		topic:         topic,
		isNoop:        isNoop,
	}
}

func checkProcessorParams(
	brokers []string,
	topic string,
	username string,
	password string,
) {
	if len(brokers) == 0 {
		panic("empty kafka brokers")
	}
	if topic == "" {
		panic("missing dlq kafka topic")
	}

	if username == "" || password == "" {
		panic("kafka username/password are mandatory")
	}
}

func (m dlqMiddleware) BatchProcess(ctx context.Context, messages [][]byte) error {
	const op = errors.Op("implgoduckprocessor.dlqMiddleware.BatchProcess")
	err := m.nextBatch.BatchProcess(ctx, messages)
	if err == nil {
		return nil
	}

	if errors.GetSeverity(err) == errors.SeverityFatal {
		return err
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	if m.isNoop {
		log.Info().
			Err(err).
			Msg("Not sending message batch to dlq since DLQ is configured as noop")

		return nil
	}

	log.Error().
		Err(err).
		Int("size", len(messages)).
		Msg("Sending message batch to dlq")

	err = m.sendMessage(ctx, messages...)
	if err != nil {
		return errors.E(op, err)
	}

	return nil
}

func (m dlqMiddleware) Process(ctx context.Context, message []byte) error {
	const op = errors.Op("implgoduckprocessor.dlqMiddleware.Process")
	err := m.nextSingle.Process(ctx, message)
	if err == nil {
		return nil
	}

	if errors.GetSeverity(err) == errors.SeverityFatal {
		return err
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	if m.isNoop {
		log.Info().
			Err(err).
			Msg("Not sending message batch to dlq since DLQ is configured as noop")

		return nil
	}

	log.Error().
		Err(err).
		Msg("Sending message batch to dlq")

	err = m.sendMessage(ctx, message)
	if err != nil {
		return errors.E(op, err)
	}

	return nil
}

func (m dlqMiddleware) sendMessage(ctx context.Context, messages ...[]byte) (err error) {
	const op = errors.Op("sendMessage")

	wg := sync.WaitGroup{}
	deliveryChan := make(chan kafka.Event, len(messages))

	once := sync.Once{}

	setErr := func(e error) {
		once.Do(func() { err = errors.E(op, e) })
	}

	go func() {
		for e := range deliveryChan {
			event, ok := e.(*kafka.Message)
			if !ok {
				continue
			}
			if event.TopicPartition.Error != nil {
				log.Ctx(ctx).Debug().
					Err(event.TopicPartition.Error).
					Str("kafka_topic", event.TopicPartition.String()).
					Msg("Failed to send message to kafka")
				setErr(err)
			} else {
				log.Ctx(ctx).Debug().
					Str("kafka_topic", event.TopicPartition.String()).
					Msg("Message sent to DLQ")
			}
			wg.Done()
		}
	}()

	for _, msg := range messages {
		err = m.kafkaProducer.Produce(
			&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &m.topic,
					Partition: kafka.PartitionAny,
				},
				Value: msg,
			}, deliveryChan)
		if err != nil {
			setErr(err)
			break
		}
		wg.Add(1)
	}

	wg.Wait()

	return err
}
