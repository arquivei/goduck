package examples

import (
	"context"
	"sync"

	"github.com/arquivei/goduck"
)

type simpleProcessor struct {
	mu       sync.Mutex
	consumed []string
}

func NewSimpleProcessor() goduck.Processor {
	return &simpleProcessor{
		consumed: []string{},
	}
}

func (s *simpleProcessor) Process(ctx context.Context, message []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consumed = append(s.consumed, string(message))
	return nil
}

func (s *simpleProcessor) messages() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.consumed...)
}
