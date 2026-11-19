# Project Guidelines

## Overview

goDuck is a Go library for workers that consume messages from ordered streams
or unordered message pools. It abstracts message retrieval, processing, and
acknowledgment through single-message and batch engines.

Module: `github.com/arquivei/goduck`.
Use the Go version declared in `go.mod`.

## Development Commands

Run commands from the repository root:

```bash
# Build and test, matching CI
go build -v ./...
go test -v ./...

# Lint using the repository configuration
golangci-lint run --timeout 3m

# Focused engine and middleware tests
go test ./engine/... ./middleware/...
```

Use golangci-lint v2.15.0, matching CI and the v2 configuration in `.golangci.yml`.
Install Lefthook and run `lefthook install` once per clone to enable the pre-commit
lint check. Run it manually with `lefthook run pre-commit --force`.

## Repository Layout

- `interfaces.go`: public message, pool, stream, and processor contracts.
- `engine/streamengine`: individual message processing, ordered per stream.
- `engine/batchstreamengine`: continuous batch processing per stream.
- `engine/jobpoolengine`: concurrent workers consuming a message pool.
- `engine/batchengine`: processes one batch and exits.
- `impl/implstream`: Confluent, Sarama, and Segmentio Kafka adapters,
  plus an in-memory implementation.
- `impl/implqueue`: Pub/Sub adapters and an in-memory implementation.
- `impl/implprocessor`: a processor implementation for tests.
- `gokithelper`: adapters from go-kit endpoints to processors.
- `middleware/`: logging, timeout, backoff, and dead-letter handling.
- `pipeline/`: composition of inputs, processors, middlewares, and sinks.
- `pipeline/inputstreams`: Kafka stream configuration and creation.
- `pipeline/*sink`: BigQuery, GCS, Elasticsearch, Pub/Sub, Kafka,
  and no-op sinks.
- `examples/`: usage examples and tests.

## Behavioral Contracts

- `Stream.Done(ctx)` acknowledges all messages retrieved so far.
  `MessagePool.Done(ctx, msg)` acknowledges one message.
- Processors must tolerate repeated processing and, depending on the engine,
  concurrent calls. Do not promise exactly-once processing.
- Retry, acknowledgment, and shutdown behavior depend on the engine and adapter.
  Read the affected implementation before changing these behaviors.
- `io.EOF` signals that the source has ended. Engines may retry other read errors.
- Error severity from `foundationkit/errors` affects retry and termination.
- Pipeline changes must account for middleware order, sink persistence,
  and shutdown handlers.

## Contribution Guidelines

- Preserve public API compatibility unless a breaking change is explicitly requested.
- Follow the affected package's constructor, option, and error-handling patterns.
- Format changed Go files with gofmt and organize imports with goimports.
- Reuse existing mocks and in-memory adapters where appropriate.
- For behavior changes, test success, failure, and relevant acknowledgment,
  retry, concurrency, and cancellation paths.
- Verify behavior against interfaces, implementations, and tests;
  the README does not describe every engine's semantics.
- Run examples that access external services only after explicitly configuring
  their resources and credentials.
- Check generated mock headers and generation tools before updating them.
  Regenerate generated files rather than editing them manually.

## Key Dependencies

- `foundationkit`: errors, retry, middlewares, and application lifecycle.
- `go-kit`: endpoints and middleware composition.
- Confluent Kafka, IBM Sarama, and Segmentio Kafka: separate Kafka adapters.
- Google Cloud Pub/Sub v2, BigQuery, and Storage: GCP integrations.
- Elasticsearch v7, zerolog, and testify.
