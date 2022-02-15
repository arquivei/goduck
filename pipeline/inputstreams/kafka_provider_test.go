package inputstreams

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func TestMakeStream(t *testing.T) {
	t.Run("Error: Empty broker", func(t *testing.T) {
		k := kafkaProvider{}

		s, err := k.MakeStream()
		assert.Nil(t, s)
		assert.EqualError(t, err, "panic: bad config: empty topic")

	})

	t.Run("Success", func(t *testing.T) {
		k := kafkaProvider{
			topic: []string{"some topic"},
			configMap: kafka.ConfigMap{
				"group.id": "some id",
			},
		}

		s, err := k.MakeStream()

		assert.NotNil(t, s)
		assert.NoError(t, err)
	})
}

func TestWithKafkaProvider(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		o := options{}
		err := WithKafkaProvider(
			WithKafkaTopic("my topic"),
			WithKafkaBrokers("my broker"),
			WithKafkaGroupID("my group"),
		)(&o)

		assert.NotNil(t, o.provider)
		assert.NoError(t, err)
	})

	t.Run("Error: No kafka topic", func(t *testing.T) {
		o := options{}
		err := WithKafkaProvider(
			WithKafkaBrokers("my broker"),
			WithKafkaGroupID("my group"),
		)(&o)

		assert.Nil(t, o.provider)
		assert.EqualError(t, err, ErrNoKafkaTopic.Error())

	})

	t.Run("Error: Empty kafka topic", func(t *testing.T) {
		o := options{}
		err := WithKafkaProvider(
			WithKafkaTopic(""),
			WithKafkaBrokers("my broker"),
			WithKafkaGroupID("my group"),
		)(&o)

		assert.Nil(t, o.provider)
		assert.EqualError(t, err, ErrEmptyKafkaTopic.Error())

	})
}
