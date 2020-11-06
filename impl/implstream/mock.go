package implstream

import "github.com/arquivei/goduck/impl/implstream/mockstream"

// NewMock creates a new stream mock
//
// Deprecated: use mockstream.New instead
func NewMock(items [][]byte) *mockstream.MockStream {
	return mockstream.New(items)
}

// NewDefaultStream creates a new stream mock with default elements
//
// Deprecated: use mockstream.NewDefaultStream instead
func NewDefaultStream(partition int, nElems int) *mockstream.MockStream {
	return mockstream.NewDefaultStream(partition, nElems)
}
