package pipeline

import (
	"strings"
	"time"

	"github.com/go-kit/kit/endpoint"

	"github.com/arquivei/goduck"
	"github.com/arquivei/goduck/impl/implqueue/pubsubqueue"
)

// Option applies an option to the Config struct
type Option func(*pipelineBuilderOptions)

// WithConfig takes the Config struct and configures batch size and max timeout of the pipeline
// and also configures middlewares and a DLQ.
// This is doing too much and must be replaced by other options.
func WithConfig(userConfig Config) Option {
	return func(c *pipelineBuilderOptions) {
		if err := checkConfig(&userConfig); err != nil {
			panic(err)
		}
		c.batchSize = userConfig.InputStream.BatchSize
		c.maxTimeout = time.Duration(userConfig.InputStream.MaxTimeoutMilli) * time.Millisecond

		if userConfig.InputStream.DLQKafkaTopic != "" {
			c.dlq.brokers = strings.Split(userConfig.Kafka.Brokers, ",")
			c.dlq.topic = userConfig.InputStream.DLQKafkaTopic
			c.dlq.username = userConfig.Kafka.Username
			c.dlq.password = userConfig.Kafka.Password
		}

		withMessagePoolConfig(userConfig.MessagePool)(c)

		c.middlewares = append(c.middlewares, getMiddlewares(userConfig)...)
		c.engineType = userConfig.EngineType
	}
}

// WithEndpoint adds an endpoint to the Config struct
func WithEndpoint(e endpoint.Endpoint) Option {
	return func(c *pipelineBuilderOptions) {
		c.endpoint = e
	}
}

// WithInputStreams adds an input stream to the Config struct
func WithInputStreams(s ...goduck.Stream) Option {
	return func(c *pipelineBuilderOptions) {
		c.inputStreams = s
	}
}

// WithProcessor adds a processor to the config struct
func WithProcessor(p goduck.Processor) Option {
	return func(c *pipelineBuilderOptions) {
		c.processor = p
	}
}

// WithBatchProcessor adds a batchProcessor to the config struct
func WithBatchProcessor(p goduck.BatchProcessor) Option {
	return func(c *pipelineBuilderOptions) {
		c.batchProcessor = p
	}
}

// WithBatchDecoder adds a batch decoder to the Config struct
func WithBatchDecoder(d goduck.EndpointBatchDecoder) Option {
	return func(c *pipelineBuilderOptions) {
		c.batchDecoder = d
	}
}

// WithDecoder adds a decoder to the Config struct
func WithDecoder(d goduck.EndpointDecoder) Option {
	return func(c *pipelineBuilderOptions) {
		c.decoder = d
	}
}

// WithSink adds a sink to the Config struct
func WithSink(s Sink, e SinkEncoder) Option {
	return func(c *pipelineBuilderOptions) {
		c.sink = s
		c.sinkEncoder = e
	}
}

// WithMiddlewares chains the specified middlewares on the transformation and
// sink layers
func WithMiddlewares(
	middlewares ...endpoint.Middleware,
) Option {
	return func(c *pipelineBuilderOptions) {
		c.middlewares = append(c.middlewares, middlewares...)
	}
}

func withMessagePoolConfig(userConfig MessagePoolConfig) Option {
	return func(c *pipelineBuilderOptions) {
		if userConfig.Provider == "" {
			return
		}
		switch userConfig.Provider {
		case "pubsub":
			pool, err := pubsubqueue.New(userConfig.Pubsub)
			if err != nil {
				panic(err)
			}
			c.messagePool = pool
			c.nPoolWorkers = userConfig.NWorkers
		default:
			panic("invalid message pool provider: " + userConfig.Provider)
		}
	}
}
