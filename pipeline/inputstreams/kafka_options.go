package inputstreams

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// KafkaOption configures the kafka provider.
type KafkaOption func(*kafkaProvider)

// WithKafkaTopic sets the kafka topic or topics.
func WithKafkaTopic(topics ...string) KafkaOption {
	return func(kp *kafkaProvider) {
		kp.topic = topics
	}
}

// WithKafkaConfigMap configures goducks inner librdkafka.
// All values in the provided ConfigMap are copied to the ConfigMap
// inside the provider. So it's to call this with maps with different keys,
// otherwise keys will be replaced.
func WithKafkaConfigMap(cm kafka.ConfigMap) KafkaOption {
	return func(kp *kafkaProvider) {
		// Nothing to add
		if len(cm) == 0 {
			return
		}

		for key, value := range cm {
			kp.configMap[key] = value
		}
	}
}

// WithKafkaConfigValue configures goducks inner librdkafka.
// The provided value sets or replaces the existing value in the ConfigMap
// inside the provider.
func WithKafkaConfigValue(name string, value kafka.ConfigValue) KafkaOption {
	return func(kp *kafkaProvider) {
		kp.configMap[name] = value
	}
}

// WithKafkaSaslPlainAuthentication configures kafka sasl authentication.
func WithKafkaSaslPlainAuthentication(username, password string) KafkaOption {
	return func(kp *kafkaProvider) {
		kp.configMap["security.protocol"] = "sasl_plaintext"
		kp.configMap["sasl.mechanisms"] = "PLAIN"
		kp.configMap["sasl.username"] = username
		kp.configMap["sasl.password"] = password
	}
}

// WithKafkaPlaintextAuthentication configures kafka plaintext authentication.
func WithKafkaPlaintextAuthentication(username, password string) KafkaOption {
	return func(kp *kafkaProvider) {
		kp.configMap["security.protocol"] = "PLAINTEXT"
		kp.configMap["sasl.mechanisms"] = "PLAIN"
		kp.configMap["sasl.username"] = username
		kp.configMap["sasl.password"] = password
	}
}

// WithKafkaBrokers sets the kafka topics for the input stream.
func WithKafkaBrokers(brokers ...string) KafkaOption {
	return func(kp *kafkaProvider) {
		kp.configMap["bootstrap.servers"] = strings.Join(brokers, ",")
	}
}

// WithKafkaGroupID sets the kafka group id.
func WithKafkaGroupID(id string) KafkaOption {
	return func(kp *kafkaProvider) {
		kp.configMap["group.id"] = id
	}
}
