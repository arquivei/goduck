package examples

import (
	"context"
	"testing"

	"github.com/arquivei/goduck/engine/jobpoolengine"
	"github.com/arquivei/goduck/impl/implqueue"
)

func TestSimpleQueue(t *testing.T) {
	processor := NewSimpleProcessor().(*simpleProcessor)
	dataset := newSimpleTestDataset()

	queue := implqueue.NewQueue(dataset.makeBytes())
	engine := jobpoolengine.New(queue, processor, 2)
	engine.Run(context.Background())
	dataset.validate(processor.consumed)
}
