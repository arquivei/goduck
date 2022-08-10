package pipeline

import (
	"time"

	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck/impl/implqueue/pubsubqueue"
)

// Config contains the parameters for a general purpose pipeline. This
// should be set by the user that is running the pipeline.
// This goes nicely with app.SetupConfig().
type Config struct {
	SystemName  string
	InputStream struct {
		Provider string
		Kafka    struct {
			Topic    string
			GroupID  string
			NWorkers int
		}
		BatchSize           int `default:"1"`
		MaxTimeoutMilli     int `default:"0"`
		CommitIntervalMilli int `default:"0"`

		ProcessingTimeoutMilli int `default:"60000"`
		MaxProcessingRetries   int `default:"10"`
		DLQKafkaTopic          string
	}
	MessagePool MessagePoolConfig
	StaleAfter  time.Duration
	Kafka       struct {
		Brokers  string
		Username string
		Password string `secret:"true"`
	}
}

// MessagePoolConfig TODO
type MessagePoolConfig struct {
	Provider string
	NWorkers int `default:"1"`
	Pubsub   pubsubqueue.PubsubConfigs
}

func checkConfig(config *Config) error {
	const op = errors.Op("checkConfig")
	if config.SystemName == "" {
		return errors.E(op, ErrSystemNameEmpty)
	}
	return nil
}
