package kafkasarama

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/IBM/sarama"
	"github.com/arquivei/foundationkit/errors"
	"github.com/imkira/go-observer"
	"github.com/rs/zerolog/log"
)

type internalMessage struct {
	msg  *sarama.ConsumerMessage
	done chan struct{}
}

type consumerGroupHandler struct {
	session     sarama.ConsumerGroupSession
	sessionLock *sync.RWMutex

	msgChan             chan internalMessage
	msgChanLock         *sync.RWMutex
	msgChanIsOpen       bool
	lastUnackedMessages map[string]*sarama.ConsumerMessage

	hasInFlightMessages observer.Property

	done chan struct{}
}

func newHandler() *consumerGroupHandler {
	return &consumerGroupHandler{
		sessionLock: &sync.RWMutex{},

		msgChan:             make(chan internalMessage),
		msgChanLock:         &sync.RWMutex{},
		msgChanIsOpen:       true,
		lastUnackedMessages: map[string]*sarama.ConsumerMessage{},

		hasInFlightMessages: observer.NewProperty(false),

		done: make(chan struct{}),
	}
}

func (h *consumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.sessionLock.Lock()
	defer h.sessionLock.Unlock()
	h.session = session
	return nil
}
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	h.waitForDone()
	h.eraseSession()
	return nil
}

func (h *consumerGroupHandler) eraseSession() {
	h.sessionLock.Lock()
	defer h.sessionLock.Unlock()
	h.session = nil
}

func (h *consumerGroupHandler) waitForDone() {
	valueObserver := h.hasInFlightMessages.Observe()
	hasInflightMessages := valueObserver.Value().(bool)
	for hasInflightMessages {
		hasInflightMessages = valueObserver.WaitNext().(bool)
	}
}

func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.msgChanLock.RLock()
	defer h.msgChanLock.RUnlock()

	if !h.msgChanIsOpen {
		return nil
	}

	for consumerMessage := range claim.Messages() {
		msg := internalMessage{
			msg:  consumerMessage,
			done: make(chan struct{}),
		}

		select {
		case h.msgChan <- msg:
			// waits for message to be successfully stored
			<-msg.done
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
	case internalMsg, ok := <-h.msgChan:
		if !ok {
			return nil, io.EOF
		}
		h.storeLastMessage(internalMsg.msg)
		close(internalMsg.done)
		return internalMsg.msg, nil
	}
}

func (h *consumerGroupHandler) storeLastMessage(msg *sarama.ConsumerMessage) {
	key := fmt.Sprintf("%s:%d", msg.Topic, msg.Partition)
	h.lastUnackedMessages[key] = msg
	h.hasInFlightMessages.Update(true)
}

func (h *consumerGroupHandler) Done() {
	h.sessionLock.RLock()
	defer h.sessionLock.RUnlock()

	if h.session == nil {
		return
	}
	for _, msg := range h.lastUnackedMessages {
		h.session.MarkMessage(msg, "")
	}

	h.hasInFlightMessages.Update(false)
}

func (h *consumerGroupHandler) Close() {
	const op = errors.Op("kafkasarama.consumerGroupHandler.Close")
	defer func() {
		// this method can be called more than once, which would panic
		if err := recover(); err != nil {
			log.Debug().Err(errors.E(op, err)).Msg("Attempting to close twice")
		}
	}()

	// Inform threads to stop pushing messages
	close(h.done)

	// Wait for them to stop
	h.msgChanLock.Lock()
	defer h.msgChanLock.Unlock()

	// No further messages will be sent
	close(h.msgChan)

}
