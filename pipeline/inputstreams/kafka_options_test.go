package inputstreams

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
)

func TestWithKafkaTopic(t *testing.T) {
	k := kafkaProvider{}

	WithKafkaTopic("t1")(&k)
	assert.Equal(t, []string{"t1"}, k.topic)

	WithKafkaTopic("t2", "t3")(&k)
	assert.Equal(t, []string{"t2", "t3"}, k.topic)
}

func TestWithKafkaConfigMap(t *testing.T) {
	k := kafkaProvider{
		configMap: kafka.ConfigMap{},
	}

	WithKafkaConfigMap(nil)(&k)
	assert.Empty(t, k.configMap)

	myConfig := kafka.ConfigMap{
		"key1": "value1",
		"key2": "value2",
	}

	WithKafkaConfigMap(myConfig)(&k)

	assert.Equal(t, myConfig, k.configMap)
}

func TestWithKafkaConfigValue(t *testing.T) {
	k := kafkaProvider{
		configMap: kafka.ConfigMap{},
	}

	WithKafkaConfigValue("key", "value")(&k)

	assert.Equal(t, "value", k.configMap["key"])
}

func TestWithKafkaSaslPlainAuthentication(t *testing.T) {
	k := kafkaProvider{
		configMap: kafka.ConfigMap{},
	}

	WithKafkaSaslPlainAuthentication("user", "pass")(&k)

	assert.Equal(t, "sasl_plaintext", k.configMap["security.protocol"])
	assert.Equal(t, "PLAIN", k.configMap["sasl.mechanisms"])
	assert.Equal(t, "user", k.configMap["sasl.username"])
	assert.Equal(t, "pass", k.configMap["sasl.password"])
}

func TestWithKafkaSSLAuthentication(t *testing.T) {
	k := kafkaProvider{
		configMap: kafka.ConfigMap{},
	}

	WithKafkaSSLAuthentication("user", "pass", "cert_path")(&k)

	assert.Equal(t, "sasl_ssl", k.configMap["security.protocol"])
	assert.Equal(t, "PLAIN", k.configMap["sasl.mechanisms"])
	assert.Equal(t, "user", k.configMap["sasl.username"])
	assert.Equal(t, "pass", k.configMap["sasl.password"])
	assert.Equal(t, "cert_path", k.configMap["ssl.ca.location"])
}

func TestWithKafkaBrokers(t *testing.T) {
	k := kafkaProvider{
		configMap: kafka.ConfigMap{},
	}

	WithKafkaBrokers("broker1")(&k)

	assert.Equal(t, "broker1", k.configMap["bootstrap.servers"])
	WithKafkaBrokers("broker1", "broker2", "broker3")(&k)

	assert.Equal(t, "broker1,broker2,broker3", k.configMap["bootstrap.servers"])
}

func TestWithKafkaGroupID(t *testing.T) {
	k := kafkaProvider{
		configMap: kafka.ConfigMap{},
	}

	WithKafkaGroupID("group")(&k)

	assert.Equal(t, "group", k.configMap["group.id"])
}
