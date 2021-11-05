package inputstreams

import (
	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck"
	"github.com/arquivei/goduck/impl/implstream/kafkaconfluent"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type kafkaProvider struct {
	topic     []string
	configMap kafka.ConfigMap
}

// WithKafkaProvider configures the input stream with a kafka provider.
func WithKafkaProvider(opts ...KafkaOption) Option {
	return func(o *options) error {
		// Create kafka provider
		provider := &kafkaProvider{
			configMap: kafka.ConfigMap{
				"auto.offset.reset":        "earliest",
				"enable.auto.offset.store": "false",
			},
		}
		for _, opt := range opts {
			opt(provider)
		}
		if len(provider.topic) == 0 {
			return ErrNoKafkaTopic
		}
		for _, t := range provider.topic {
			if t == "" {
				return ErrEmptyKafkaTopic
			}
		}

		o.provider = provider

		return nil
	}
}

func (p *kafkaProvider) MakeStream() (s goduck.Stream, err error) {
	// KLUDGE: goduck doesn't provide a constructor that doesn't panic.
	defer func() {
		if r := recover(); r != nil {
			err = errors.NewFromRecover(r)
		}
	}()

	s = kafkaconfluent.MustNew( // FIXME: Add new on goduck and refactor here
		kafkaconfluent.Config{
			Topics:        p.topic,
			RDKafkaConfig: &p.configMap,
		},
	)

	return s, nil
}
