package examples

import (
	"context"

	"github.com/arquivei/goduck"
)

type simpleProcessor struct {
	consumed []string
}

func NewSimpleProcessor() goduck.Processor {
	return &simpleProcessor{
		consumed: []string{},
	}
}

func (s *simpleProcessor) Process(ctx context.Context, message []byte) error {
	s.consumed = append(s.consumed, string(message))
	return nil
}
