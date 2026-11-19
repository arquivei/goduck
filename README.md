# goDuck

goDuck is a Go library for message-processing workers. It reads messages from
ordered streams or unordered message pools, passes their bytes to a processor,
and coordinates acknowledgment according to the selected engine.

Use the low-level engines directly, or use the `pipeline` package to compose
inputs, go-kit endpoints, middlewares, and output sinks.

## Installation

Use the Go version declared in [go.mod](go.mod), then add the module to your
application:

```bash
go get github.com/arquivei/goduck
```

Building packages that use the Confluent Kafka client requires CGO and a C
compiler. Keep `CGO_ENABLED=1` when building the entire repository.

## Core Interfaces

The public contracts are defined in [interfaces.go](interfaces.go):

- `RawMessage.Bytes()` exposes a message payload.
- `Processor.Process(ctx, message)` processes one payload.
- `BatchProcessor.BatchProcess(ctx, messages)` processes a batch.
- `Stream.Next(ctx)` reads the next message; `Done(ctx)` acknowledges all messages
  retrieved so far from that stream.
- `MessagePool.Next(ctx)` reads a message; `Done(ctx, msg)` and `Failed(ctx, msg)`
  mark that individual message as completed or failed.
- Sources signal exhaustion with `io.EOF` and expose `Close()` for cleanup.

Processors should be idempotent: messages can be delivered more than once.
Processors shared by multiple workers must also be safe for concurrent calls.
The exact delivery guarantees depend on the engine, adapter, broker settings,
and acknowledgment outcome; goDuck does not provide exactly-once processing.

## Stream Example

This complete program uses the in-memory stream, so it needs no broker or cloud
credentials. Save it as `main.go` in an application that depends on goDuck and
run `go run .`:

```go
package main

import (
	"context"
	"log"

	"github.com/arquivei/goduck"
	"github.com/arquivei/goduck/engine/streamengine"
	"github.com/arquivei/goduck/impl/implstream"
)

type processor struct{}

func (processor) Process(_ context.Context, message []byte) error {
	log.Printf("received: %s", message)
	return nil
}

func main() {
	stream := implstream.NewMock([][]byte{
		[]byte("first message"),
		[]byte("second message"),
	})
	defer stream.Close()

	engine := streamengine.New(processor{}, []goduck.Stream{stream})
	if err := engine.Run(context.Background()); err != nil {
		log.Print(err)
	}
}
```

`streamengine` starts one worker per supplied stream and processes messages
sequentially within each stream. Multiple streams can run concurrently; there
is no global ordering guarantee across them.

## Message Pool Example

The following integration function accepts a configured message pool and
processor. The caller owns the pool's lifecycle and chooses how to coordinate
shutdown with in-flight work:

```go
package worker

import (
	"context"

	"github.com/arquivei/goduck"
	"github.com/arquivei/goduck/engine/jobpoolengine"
)

func Run(ctx context.Context, pool goduck.MessagePool, processor goduck.Processor) error {
	engine := jobpoolengine.New(pool, processor, 4)
	return engine.Run(ctx)
}
```

For Google Cloud Pub/Sub, create the pool with
`pubsubqueue.New(pubsubqueue.PubsubConfigs{ProjectID: projectID, Subscription: subscriptionID})`
from `github.com/arquivei/goduck/impl/implqueue/pubsubqueue`. Handle its returned
error before starting the engine, and configure Google Cloud credentials and
access to the subscription. This adapter uses the Pub/Sub v2 client.

Unlike streams, a message pool makes no ordering guarantee. `jobpoolengine`
marks successful messages with `Done` and failed messages with `Failed`.

## Choosing an Engine

| Package under `engine/` | Processor | Execution model |
| --- | --- | --- |
| `streamengine` | `Processor` | Continuous, one worker per stream |
| `batchstreamengine` | `BatchProcessor` | Continuous batches, one worker per stream |
| `jobpoolengine` | `Processor` | Configurable worker count over one message pool |
| `batchengine` | `BatchProcessor` | Reads and processes one batch, then returns |

The batch engines accept a maximum batch size and a timeout for collecting a
batch. This collection timeout does not limit processor execution time.

### Errors and Acknowledgment

- `streamengine` and `batchstreamengine` retry processing errors other than
  `foundationkit/errors.SeverityFatal`. They acknowledge only after processing
  succeeds. Fatal processing errors stop the engine and are returned by `Run`.
- `jobpoolengine` marks any processing error as failed. A fatal processing error
  also cancels polling. Redelivery is controlled by the pool implementation.
- `batchengine` invokes the batch processor once. A fatal error prevents
  acknowledgment and is returned by `Run`. A non-fatal processing error can
  still be followed by `Done`; this engine does not retry that batch.
- The engines currently ignore errors returned by `Done` and `Failed`.
  A successful `Run` therefore does not prove that acknowledgments were persisted.

Use the existing backoff, timeout, and dead-letter middlewares where appropriate
for your application's failure policy. Inspect their configuration and the
selected engine rather than assuming all failures have the same retry behavior.

### Shutdown

Cancel the context passed to `Run` to request shutdown of continuous engines.
The engines invoke processors with `context.Background()`, so canceling the
engine context does not directly cancel an in-flight processor call. Use a
processor timeout or application-level coordination when needed.

`streamengine` and `batchstreamengine` wait for their stream workers to return.
`jobpoolengine.Run` returns after polling stops and does not wait for all worker
goroutines to finish. The engines do not call the sources' `Close` methods;
the application must manage source cleanup.

## Kafka Adapters

Choose an adapter explicitly; their configuration types are not interchangeable.

| Package under `impl/implstream/` | Constructor |
| --- | --- |
| `kafkaconfluent` | `New(Config)` or `NewWithAuth(Config)` |
| `kafkasarama` | `MustNewKafkaStream(KafkaConfigs)` or `MustNewKafkaStreamWithSaramaConfigs(...)` |
| `kafkasegmentio` | `NewKafkaStream(KafkaConfigs)` or `NewKafkaStreamWithCustomConfigs(...)` |

Constructors prefixed with `Must` panic on initialization errors.

For the Segmentio adapter, `KafkaConfigs.CommitInterval` controls offset commits:
zero commits synchronously, while a positive interval commits asynchronously.
It does not configure concurrent processor execution. A longer interval can
increase the number of processed messages replayed after a failure before the
next commit. Sarama and Confluent have their own commit settings; this field does
not apply to them.

## Pipelines and Sinks

The [pipeline](pipeline) package connects input streams or a message pool to a
processor and an output sink. It supports go-kit endpoint decoding, middleware
composition, batching, and application shutdown integration through foundationkit.

Available sink packages are `bigquerysink`, `gcssink`, `elasticsink`, `pubsubsink`,
`kafkasink`, and `noopsink`. See [pipeline options](pipeline/pipeline_options.go)
and [configuration](pipeline/config.go) for the supported setup, and the
[BigQuery example](pipeline/bigquerysink/example/main.go) for a sink usage example.
External integrations require their own credentials and resources.

## Development

Run these commands from the repository root:

```bash
go build -v ./...
go test -v ./...
golangci-lint run --timeout 3m
```

Use [golangci-lint v2.15.0](https://golangci-lint.run/docs/welcome/install/local/),
matching CI. Format changed Go files with `golangci-lint fmt`, which runs the
configured gofmt and goimports formatters.

### Pre-commit Hook

Install [Lefthook](https://lefthook.dev/install/) (tested with
v2.2.2), then enable the hook once per clone:

```bash
lefthook install
```

The pre-commit hook runs `golangci-lint run --timeout 3m` across the entire
repository and blocks the commit if lint fails. It does not modify or stage files.
You can run the same check manually:

```bash
lefthook run pre-commit --force
```

See [examples](examples) for additional usage and [AGENTS.md](AGENTS.md) for
repository guidance for AI assistants.

## License

[BSD 3-Clause](LICENSE).
