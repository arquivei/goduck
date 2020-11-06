package implstream

import (
	"time"

	"github.com/arquivei/goduck"
	"github.com/arquivei/goduck/impl/implstream/kafkasegmentio"

	kafkaGo "github.com/segmentio/kafka-go"
)

// KafkaConfigs contains the configs for kafka connection
type KafkaConfigs struct {
	Brokers  []string
	Topic    string
	GroupID  string
	Username string
	Password string

	// SegmentIO configs

	CommitInterval time.Duration
}

// NewKafkaStream creates a new goduck.Stream from kafka, using segmentio library
// Deprecated: use kafkasegmentio.New instead
func NewKafkaStream(config KafkaConfigs) goduck.Stream {
	return kafkasegmentio.New(kafkasegmentio.Configs{
		Brokers:        config.Brokers,
		Topic:          config.Topic,
		GroupID:        config.GroupID,
		Username:       config.Username,
		Password:       config.Password,
		CommitInterval: config.CommitInterval,
	})
}

// NewKafkaStreamWithCustomConfigs allows creating a kafka connection with all
// configs from the SegmentIO library
// Deprecated: use kafkasegmentio.NewWithCustomConfigs instead
func NewKafkaStreamWithCustomConfigs(config kafkaGo.ReaderConfig) goduck.Stream {
	return kafkasegmentio.NewWithCustomConfigs(config)
}
