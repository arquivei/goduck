package main

import (
	"context"

	"github.com/arquivei/goduck"
	"github.com/arquivei/goduck/engine/streamengine/v2"
	"github.com/arquivei/goduck/impl/implstream/kafkaconfluent"
)

func main() {
	// First, we need to create a new stream.
	stream := kafkaconfluent.MustNew(kafkaconfluent.Config{
		Brokers:  []string{"localhost:9092"},
		GroupID:  "my-group",
		Username: "my-username",
		Password: "my-password",
		Topics:   []string{"my-topic"},
	})

	// Second, we need to create a new engine to correctly poll the stream,
	// while maintaining the ordering guarantees.
	engine, err := streamengine.NewFromEndpoint(Do, Decode, []goduck.Stream{stream})
	if err != nil {
		panic(err)
	}

	// Finally, we can run the engine.
	engine.Run(context.Background())

}
