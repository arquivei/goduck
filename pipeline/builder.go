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
	"github.com/arquivei/goduck/engine/batchengine"
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
	case shouldBuildWithRunOnceEngine(c):
		err = buildWithRunOnceEngine(c, p)
	case shouldBuildWithMessagePoolEngine(c) && shouldBuildWithSomeStreamEngine(c):
		// sanity check
		// This is already checked by checkPipelineBuilderOptions()
		err = ErrBothInputSet
	case shouldBuildWithMessagePoolEngine(c):
		err = buildWithMessagePoolEngine(c, p)
	case shouldBuildWithSomeStreamEngine(c):
		err = buildWithSomeStreamEngine(c, p)
	default:
		// sanity check
		// This is already checked by checkPipelineBuilderOptions()
		err = ErrEmptyInputStreamOrMessagePool
	}

	if err != nil {
		return nil, errors.E(op, err)
	}

	return p, nil
}

func shouldBuildWithMessagePoolEngine(c pipelineBuilderOptions) bool {
	return c.messagePool != nil
}

func shouldBuildWithSomeStreamEngine(c pipelineBuilderOptions) bool {
	return len(c.inputStreams) > 0
}

func shouldBuildWithRunOnceEngine(c pipelineBuilderOptions) bool {
	return c.engineType == EngineTypeRunOnce
}

func buildWithBachStreamEngine(internalConfig pipelineBuilderOptions, pipe *pipeline) error {
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
			internalConfig.dlq.isNoop,
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

func buildWithSomeStreamEngine(builderOpts pipelineBuilderOptions, pipe *pipeline) error {
	if builderOpts.batchDecoder != nil {
		return buildWithBachStreamEngine(builderOpts, pipe)
	}
	return buildWithStreamEngine(builderOpts, pipe)
}

func buildWithStreamEngine(builderOpts pipelineBuilderOptions, pipe *pipeline) error {
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
			builderOpts.dlq.isNoop,
		)
	}

	pipe.engine = streamengine.New(
		processor,
		builderOpts.inputStreams,
	)
	return nil
}

func buildWithMessagePoolEngine(builderOpts pipelineBuilderOptions, pipe *pipeline) error {
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

func buildWithRunOnceEngine(internalConfig pipelineBuilderOptions, pipe *pipeline) error {
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
			internalConfig.dlq.isNoop,
		)
	}

	pipe.engine = batchengine.New(
		processor,
		internalConfig.batchSize,
		internalConfig.maxTimeout,
		internalConfig.inputStreams[0],
	)
	return nil
}

func getMiddlewares(config Config) []endpoint.Middleware {
	timeoutConfig := timeoutmiddleware.Config{
		Timeout:       time.Duration(config.InputStream.ProcessingTimeoutMilli) * time.Millisecond,
		Wait:          true,
		ErrorSeverity: errors.SeverityFatal,
	}
	retryBackoffConfig := newBackoffmiddlewareConfig(config)
	loggingConfig := loggingmiddleware.NewDefaultConfig(config.SystemName)

	e := []endpoint.Middleware{
		trackingmiddleware.New(),
		gokitmiddlewares.Must(timeoutmiddleware.New(timeoutConfig)),
		backoffmiddleware.New(retryBackoffConfig),
		loggingmiddleware.MustNew(loggingConfig),
	}
	if config.StaleAfter > 0 {
		c := stalemiddleware.NewDefaultConfig(app.HealthinessProbeGroup())
		c.MaxTimeBetweenRequests = config.StaleAfter
		e = append(e, stalemiddleware.New(c))
		log.Warn().Msgf("[goduck][pipeline] Stale middleware is active. The system will be set to unhealthy if no message is received in %s.", config.StaleAfter.String())
	}

	return e
}

// newBackoffmiddlewareConfig returns a backoffmiddleware.Config with the values
// from the Config struct. If the values are not set, the default values are used.
// It will be used to create the backoffmiddleware.
func newBackoffmiddlewareConfig(config Config) backoffmiddleware.Config {
	return backoffmiddleware.Config{
		InitialDelay: config.Backoffmiddleware.InitialDelay,
		MaxDelay:     config.Backoffmiddleware.MaxDelay,
		Spread:       config.Backoffmiddleware.Spread,
		Factor:       config.Backoffmiddleware.Factor,
		MaxRetries:   config.Backoffmiddleware.MaxRetries,
	}
}
