package implqueue

import (
	"context"
	"io"
	"strconv"
	"sync"

	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck"
)

type MockQueue struct {
	items         []goduck.RawMessage
	consumedItems []bool
	currentIdx    int
	mtx           *sync.Mutex
}

func NewMock(items [][]byte) *MockQueue {
	consumedItems := make([]bool, len(items))
	messages := make([]goduck.RawMessage, len(items))

	for i := 0; i < len(items); i++ {
		consumedItems[i] = false
		messages[i] = &mockRawMessage{
			data: items[i],
			idx:  i,
		}
	}
	return &MockQueue{
		items:         messages,
		consumedItems: consumedItems,
		currentIdx:    0,
		mtx:           &sync.Mutex{},
	}
}

func NewDefaultQueue(nElems int) *MockQueue {
	messages := make([][]byte, nElems)

	for i := 0; i < nElems; i++ {
		messages[i] = []byte(strconv.Itoa(i))
	}
	return NewMock(messages)
}

func (m *MockQueue) Next(ctx context.Context) (goduck.RawMessage, error) {
	const op = errors.Op("MockQueue.Pool")
	m.mtx.Lock()
	defer m.mtx.Unlock()

	nElems := len(m.consumedItems)

	for tries := 0; tries < nElems; tries++ {
		if !m.consumedItems[m.currentIdx] {
			msg := m.items[m.currentIdx]
			m.currentIdx = (m.currentIdx + 1) % nElems
			return msg, nil
		}
		m.currentIdx = (m.currentIdx + 1) % nElems
	}

	return nil, io.EOF
}

func (m MockQueue) Done(ctx context.Context, msg goduck.RawMessage) error {
	const op = errors.Op("MockQueue.Done")
	m.mtx.Lock()
	defer m.mtx.Unlock()
	rawMsg, ok := msg.(*mockRawMessage)
	if !ok {
		return errors.E(op, "type cast error")
	}
	m.consumedItems[rawMsg.idx] = true
	return nil
}
func (m MockQueue) Failed(ctx context.Context, msg goduck.RawMessage) error {
	const op = errors.Op("MockQueue.Failed")
	m.mtx.Lock()
	defer m.mtx.Unlock()
	rawMsg, ok := msg.(*mockRawMessage)
	if !ok {
		return errors.E(op, "type cast error")
	}
	if m.consumedItems[rawMsg.idx] {
		return errors.E(op, "message status was 'Done'")
	}
	return nil
}

func (m MockQueue) Close() error {
	return nil
}

// IsEmpty tests if all elements in the queue are consumed
func (m MockQueue) IsEmpty() bool {
	for i := 0; i < len(m.consumedItems); i++ {
		if !m.consumedItems[i] {
			return false
		}
	}
	return true
}
