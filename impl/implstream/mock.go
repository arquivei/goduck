package implstream

import (
	"context"
	"io"
	"strconv"
	"sync"

	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck"
)

type MockStream struct {
	items         []goduck.RawMessage
	nElems        int
	offset        int
	sessionOffset int
	mtx           *sync.Mutex
}

func NewStream(items [][]byte) *MockStream {
	nElems := len(items)
	messages := make([]goduck.RawMessage, len(items))

	for i := 0; i < len(items); i++ {
		messages[i] = &mockRawMessage{
			data: items[i],
			idx:  i,
		}
	}
	return &MockStream{
		items:         messages,
		nElems:        nElems,
		offset:        0,
		sessionOffset: 0,
		mtx:           &sync.Mutex{},
	}
}
func NewDefaultStream(partition int, nElems int) *MockStream {
	messages := make([][]byte, nElems)
	prefix := strconv.Itoa(partition) + ","
	for i := 0; i < nElems; i++ {
		messages[i] = []byte(prefix + strconv.Itoa(i))
	}
	return NewStream(messages)
}

func (m *MockStream) Poll(ctx context.Context) (goduck.RawMessage, error) {
	const op = errors.Op("MockStream.Pool")
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.sessionOffset == m.nElems {
		return nil, io.EOF
	}
	msg := m.items[m.sessionOffset]
	m.sessionOffset++
	return msg, nil
}

func (m *MockStream) Done(ctx context.Context) error {
	const op = errors.Op("MockStream.Done")
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.offset = m.sessionOffset
	return nil
}

func (m MockStream) Close() error {
	return nil
}

// IsEmpty tests if all elements in the queue are consumed
func (m MockStream) IsEmpty() bool {
	return m.nElems == m.offset
}
