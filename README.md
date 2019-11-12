## goDuck

This project's purpose is to be an engine that abstract message dispatching for workers
that deals with the concept of either streams or pools. 
In other words, reading a message from a stream or a pool, and delivering that message
through the **Processor** Interface and interpreting its return value.

>It is important to note that, if the Process function returns an error, the engine wont 
Ack the message, thus, not removing it from the queue or stream. The main idea for this, 
is that the engine guarantees that every message will be processed at least once, without errors.  

Sample of a stream processor
```go
import(
	"github.com/arquivei/goduck"
	"github.com/arquivei/goduck/engine/streamengine"
)
// The engine requires a type that implements the Process function
type processor struct{}

// Process func will receive the pulled message from the engine.
func (p processor) Process(ctx context.Context, message []byte) error {
	...
    err := serviceCall(args)
    ...
	return err
}
func main {
    // call below returns a kafka abstraction (interface)
    kafka := NewKafkaStream(<your-config>)
    engine := streamengine.New(processor{}, []goduck.Stream{kafka})
    engine.Run(context.Background())
}
```

Sample of a pool processor
```go
import(
	"github.com/arquivei/goduck"
	"github.com/arquivei/goduck/engine/streamengine"
)
// The engine requires a type that implements the Process function
type processor struct{}

// Process func will receive the pulled message from the engine.
func (p processor) Process(ctx context.Context, message []byte) error {
	...
    err := serviceCall(args)
    ...
	return err
}


func main {
    // call below returns a pubsub abstraction (interface)
    pubsub, err := NewPubsubQueue(<your-config>) 
    if err != nil {
        <handle err>
    }
    engine := jobpoolengine.New(pubsub, processor{}, 1)
    engine.Run(context.Background())
}
```

To terminate the engine execution, a simple context cancellation will perform a shutdown
of the application.