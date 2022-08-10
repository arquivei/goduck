package pipeline

import (
	"time"

	"github.com/arquivei/goduck/middleware/dlqmiddleware"

	"github.com/arquivei/foundationkit/app"
	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/foundationkit/gokitmiddlewares"
	"github.com/arquivei/foundationkit/gokitmiddlewares/backoffmiddleware"
	"github.com/arquivei/foundationkit/gokitmiddlewares/loggingmiddleware"
	"github.com/arquivei/foundationkit/gokitmiddlewares/stalemiddleware"
	"github.com/arquivei/foundationkit/gokitmiddlewares/timeoutmiddleware"
	"github.com/arquivei/foundationkit/gokitmiddlewares/trackingmiddleware"
	"github.com/arquivei/goduck/engine/batchstreamengine"
	"github.com/arquivei/goduck/engine/jobpoolengine"
	"github.com/arquivei/goduck/engine/streamengine"
	"github.com/arquivei/goduck/gokithelper"
	"github.com/go-kit/kit/endpoint"
	"github.com/rs/zerolog/log"
)

func build(c pipelineBuilderOptions) (Pipeline, error) {
	const op = errors.Op("new")

	err := checkPipelineBuilderOptions(c)
	if err != nil {
		return nil, errors.E(op, err)
	}

	p := &pipeline{done: make(chan struct{})}

	sinkMiddleware := withSink(c.sink, c.sinkEncoder)
	c.endpoint = endpoint.Chain(sinkMiddleware)(c.endpoint)
	if len(c.middlewares) > 0 {
		c.endpoint = endpoint.Chain(c.middlewares[0], c.middlewares[1:]...)(c.endpoint)
	}

	switch {
	case c.messagePool != nil:
		err = setupMessagePool(c, p)
	case len(c.inputStreams) > 0:
		err = setupStreamProcessor(c, p)
	default:
		err = errors.E(op, "invalid pipeline configuration")
	}

	if err != nil {
		return nil, err
	}

	return p, nil
}

func setupBatchProcessor(internalConfig pipelineBuilderOptions, pipe *pipeline) error {
	processor, err := gokithelper.NewEndpointBatchProcessor(
		internalConfig.endpoint,
		internalConfig.batchDecoder,
	)
	if err != nil {
		return err
	}

	if internalConfig.dlq.brokers != nil {
		processor = dlqmiddleware.WrapBatch(
			processor,
			internalConfig.dlq.brokers,
			internalConfig.dlq.topic,
			internalConfig.dlq.username,
			internalConfig.dlq.password,
		)
	}

	pipe.engine = batchstreamengine.New(
		processor,
		internalConfig.batchSize,
		internalConfig.maxTimeout,
		internalConfig.inputStreams,
	)
	return nil
}

func setupStreamProcessor(builderOpts pipelineBuilderOptions, pipe *pipeline) error {
	if builderOpts.batchDecoder != nil {
		return setupBatchProcessor(builderOpts, pipe)
	}
	return setupProcessor(builderOpts, pipe)
}

func setupProcessor(builderOpts pipelineBuilderOptions, pipe *pipeline) error {
	processor, err := gokithelper.NewEndpointProcessor(
		builderOpts.endpoint,
		builderOpts.decoder,
	)
	if err != nil {
		return err
	}

	if builderOpts.dlq.brokers != nil {
		processor = dlqmiddleware.WrapSingle(
			processor,
			builderOpts.dlq.brokers,
			builderOpts.dlq.topic,
			builderOpts.dlq.username,
			builderOpts.dlq.password,
		)
	}

	pipe.engine = streamengine.New(
		processor,
		builderOpts.inputStreams,
	)
	return nil
}

func setupMessagePool(builderOpts pipelineBuilderOptions, pipe *pipeline) error {
	processor, err := gokithelper.NewEndpointProcessor(
		builderOpts.endpoint,
		builderOpts.decoder,
	)
	if err != nil {
		return err
	}

	pipe.engine = jobpoolengine.New(
		builderOpts.messagePool,
		processor,
		builderOpts.nPoolWorkers,
	)
	return nil
}

func getMiddlewares(config Config) []endpoint.Middleware {
	timeoutConfig := timeoutmiddleware.Config{
		Timeout:       time.Duration(config.InputStream.ProcessingTimeoutMilli) * time.Millisecond,
		Wait:          true,
		ErrorSeverity: errors.SeverityFatal,
	}
	retryConfig := backoffmiddleware.NewDefaultConfig()
	loggingConfig := loggingmiddleware.NewDefaultConfig(config.SystemName)

	e := []endpoint.Middleware{
		trackingmiddleware.New(),
		gokitmiddlewares.Must(timeoutmiddleware.New(timeoutConfig)),
		backoffmiddleware.New(retryConfig),
		loggingmiddleware.MustNew(loggingConfig),
	}
	if config.StaleAfter > 0 {
		c := stalemiddleware.NewDefaultConfig(app.HealthinessProbeGroup())
		c.MaxTimeBetweenRequests = config.StaleAfter
		e = append(e, stalemiddleware.New(c))
		log.Warn().Msgf("Stale middleware is active. The system will be set to unhealthy if no message is received in %s.", config.StaleAfter.String())
	}
	return e
}
