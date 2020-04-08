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
	session     sarama.ConsumerGroupSession
	sessionLock *sync.RWMutex

	msgChan             chan *sarama.ConsumerMessage
	lastUnackedMessages map[string]*sarama.ConsumerMessage

	done      chan struct{}
	waitGroup *sync.WaitGroup
}

func newHandler() *consumerGroupHandler {
	return &consumerGroupHandler{
		sessionLock: &sync.RWMutex{},

		msgChan:             make(chan *sarama.ConsumerMessage),
		lastUnackedMessages: map[string]*sarama.ConsumerMessage{},

		done:      make(chan struct{}),
		waitGroup: &sync.WaitGroup{},
	}
}

func (h *consumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.sessionLock.Lock()
	defer h.sessionLock.Unlock()
	h.session = session
	return nil
}
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	h.sessionLock.Lock()
	defer h.sessionLock.Unlock()
	h.session = nil
	return nil
}
func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.waitGroup.Add(1)
	defer h.waitGroup.Done()
	for msg := range claim.Messages() {
		select {
		case h.msgChan <- msg:
			// successfully sent
		case <-h.done:
			// handler should stop pushing messages
			return nil
		}
	}
	return nil
}
func (h *consumerGroupHandler) Next(ctx context.Context) (*sarama.ConsumerMessage, error) {
	const op = errors.Op("kafkasarama.consumerGroupHandler.Next")
	select {
	case <-ctx.Done():
		// goduckStream.Next ctx is closed
		return nil, errors.E(op, ctx.Err())
	case msg, ok := <-h.msgChan:
		if !ok {
			return nil, io.EOF
		}
		h.storeLastMessage(msg)
		return msg, nil
	}
}

func (h *consumerGroupHandler) storeLastMessage(msg *sarama.ConsumerMessage) {
	key := fmt.Sprintf("%s:%d", msg.Topic, msg.Partition)
	h.lastUnackedMessages[key] = msg
}

func (h *consumerGroupHandler) Done() {
	h.sessionLock.RLock()
	defer h.sessionLock.RUnlock()

	for _, msg := range h.lastUnackedMessages {
		h.session.MarkMessage(msg, "")
	}
}

func (h *consumerGroupHandler) Close() {
	defer func() {
		// this method can be called more than once, which would panic
		recover()
	}()

	// Inform threads to stop pushing messages
	close(h.done)

	// Wait for them to stop
	h.waitGroup.Wait()

	// No further messages will be sent
	close(h.msgChan)

}
