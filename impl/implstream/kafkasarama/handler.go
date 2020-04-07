package kafkasarama

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/arquivei/foundationkit/errors"
)

type consumerGroupHandler struct {
	session             sarama.ConsumerGroupSession
	sessionLock         *sync.RWMutex
	msgChan             chan *sarama.ConsumerMessage
	lastUnackedMessages map[string]*sarama.ConsumerMessage
	done                chan struct{}
}

func newHandler() *consumerGroupHandler {
	return &consumerGroupHandler{
		msgChan:             make(chan *sarama.ConsumerMessage),
		sessionLock:         &sync.RWMutex{},
		done:                make(chan struct{}),
		lastUnackedMessages: map[string]*sarama.ConsumerMessage{},
	}
}

func (h *consumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.sessionLock.Lock()
	defer h.sessionLock.Unlock()
	fmt.Println("setup")
	h.session = session
	return nil
}
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	h.sessionLock.Lock()
	defer h.sessionLock.Unlock()
	fmt.Println("cleanup")
	h.session = nil
	return nil
}
func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.msgChan <- msg
	}
	return nil
}
func (h *consumerGroupHandler) Next(ctx context.Context) (*sarama.ConsumerMessage, error) {
	const op = errors.Op("kafkasarama.consumerGroupHandler.Next")
	select {
	// goduckStream.Next ctx is closed
	case <-ctx.Done():
		return nil, errors.E(op, "closed context")
	// goduckStream.run loop endded, or goduckStream.Close() was called
	case <-h.done:
		return nil, io.EOF
	// messages available
	case msg := <-h.msgChan:
		if msg == nil {
			return nil, io.EOF
		}
		h.storeLastMessage(msg)
		return msg, nil
	}
}

func (h *consumerGroupHandler) storeLastMessage(msg *sarama.ConsumerMessage) {
	key := fmt.Sprintf("%s:%s", msg.Topic, msg.Partition)
	h.lastUnackedMessages[key] = msg
}

func (h *consumerGroupHandler) Done(ctx context.Context) {
	h.sessionLock.RLock()
	defer h.sessionLock.RUnlock()

	for _, msg := range h.lastUnackedMessages {
		h.session.MarkMessage(msg, "")
	}
}
