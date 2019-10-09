package implprocessor

import (
	"context"
	"errors"
	"sync"
)

type MockProcessor struct {
	Mtx           *sync.Mutex
	JobsProcessed int
	Success       map[string]bool
	CustomFn      func()
}

func (m *MockProcessor) Process(ctx context.Context, message []byte) error {
	m.Mtx.Lock()
	defer m.Mtx.Unlock()
	strMessage := string(message)
	m.JobsProcessed++
	if m.JobsProcessed%3 == 0 {
		// Emulating intermitent failures
		return errors.New("intermitent error")
	}
	m.Success[strMessage] = true
	m.CustomFn()
	return nil
}
