package implprocessor

import (
	"context"
	"errors"
	"sync"

	"github.com/arquivei/goduck"
)

type MockProcessor struct {
	mtx           *sync.Mutex
	jobsProcessed int
	Success       map[string]bool
	CustomFn      func() error
}

func New(customFn func() error) *MockProcessor {
	return &MockProcessor{
		mtx:           &sync.Mutex{},
		jobsProcessed: 0,
		Success:       map[string]bool{},
		CustomFn:      customFn,
	}
}

func (m *MockProcessor) Process(ctx context.Context, message goduck.Message) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	strMessage := string(message.Value)
	m.jobsProcessed++
	if m.jobsProcessed%3 == 0 {
		// Emulating intermitent failures
		return errors.New("intermitent error")
	}
	m.Success[strMessage] = true
	if m.CustomFn != nil {
		return m.CustomFn()
	}
	return nil
}

func (m *MockProcessor) BatchProcess(ctx context.Context, messages []goduck.Message) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.jobsProcessed++
	if m.jobsProcessed%3 == 0 {
		// Emulating intermitent failures
		return errors.New("intermitent error")
	}
	for _, message := range messages {
		strMessage := string(message.Value)
		m.Success[strMessage] = true
	}

	if m.CustomFn != nil {
		return m.CustomFn()
	}
	return nil
}
