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

func (s *simpleProcessor) Process(ctx context.Context, message goduck.Message) error {
	s.consumed = append(s.consumed, string(message.Value))
	return nil
}
