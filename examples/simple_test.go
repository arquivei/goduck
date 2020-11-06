package examples

import (
	"context"
	"testing"

	"github.com/arquivei/goduck"
	"github.com/arquivei/goduck/engine/jobpoolengine"
	"github.com/arquivei/goduck/engine/streamengine"
	"github.com/arquivei/goduck/impl/implqueue/mockqueue"
	"github.com/arquivei/goduck/impl/implstream/mockstream"
)

func TestSimpleQueue(t *testing.T) {
	processor := NewSimpleProcessor().(*simpleProcessor)
	dataset := newSimpleTestDataset()

	queue := mockqueue.New(dataset.makeBytes())
	engine := jobpoolengine.New(queue, processor, 2)
	engine.Run(context.Background())
	dataset.validate(processor.consumed)
}

func TestSimpleStream(t *testing.T) {
	processor := NewSimpleProcessor().(*simpleProcessor)
	dataset := newSimpleTestDataset()

	stream := mockstream.New(dataset.makeBytes())
	engine := streamengine.New(processor, []goduck.Stream{stream})
	engine.Run(context.Background())
	dataset.validate(processor.consumed)
}
