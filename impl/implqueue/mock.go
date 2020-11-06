package implqueue

import (
	"github.com/arquivei/goduck/impl/implqueue/mockqueue"
)

// NewMock creates a new queue mock.
//
// Deprecated: use mockqueue.New instead
func NewMock(items [][]byte) *mockqueue.MockQueue {
	return mockqueue.New(items)
}

// NewDefaultQueue creates a new queue mock with default items.
//
// Deprecated: use mockqueue.NewDefaultQueue instead
func NewDefaultQueue(nElems int) *mockqueue.MockQueue {
	return mockqueue.NewDefaultQueue(nElems)
}
