package pubsubqueue

import (
	"context"
	"io"
	"sync"

	"cloud.google.com/go/pubsub/v2"
	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck"
)

// PubsubConfigs contains the configs for pubsub connection
type PubsubConfigs struct {
	ProjectID    string
	Subscription string
}

type pubsubConsumer struct {
	client       *pubsub.Client
	subscription *pubsub.Subscription
	nextMessage  chan *pubsub.Message
	errChannel   chan error
	closeOnce    *sync.Once
	cancelFn     func()
}

func New(config PubsubConfigs) (goduck.MessagePool, error) {
	const op = errors.Op("pubsubqueue.New")
	client, err := pubsub.NewClient(context.Background(), config.ProjectID)
	if err != nil {
		return nil, errors.E(op, err)
	}
	subscription := client.Subscription(config.Subscription)
	consumer := &pubsubConsumer{
		client:       client,
		subscription: subscription,
		nextMessage:  make(chan *pubsub.Message),
		errChannel:   make(chan error),
		closeOnce:    &sync.Once{},
	}
	go consumer.start()
	return consumer, nil
}

func (p *pubsubConsumer) start() {
	const op = errors.Op("start")
	ctx, cancelFn := context.WithCancel(context.Background())
	p.cancelFn = cancelFn
	err := p.subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		select {
		case p.nextMessage <- msg:
		case <-ctx.Done():
		}
	})
	if err != nil {
		p.errChannel <- errors.E(op, err)
	}
	p.Close()
}

func (p *pubsubConsumer) Next(ctx context.Context) (goduck.RawMessage, error) {
	const op = errors.Op("pubsubqueue.pubsubConsumer.Next")
	select {
	case err, ok := <-p.errChannel:
		if !ok {
			return nil, io.EOF
		}
		return nil, errors.E(op, err)
	case msg, ok := <-p.nextMessage:
		if !ok {
			return nil, io.EOF
		}
		rawMsg := &rawMessage{msg}
		return rawMsg, nil
	case <-ctx.Done():
		return nil, io.EOF
	}
}
func (p *pubsubConsumer) Done(ctx context.Context, msg goduck.RawMessage) error {
	const op = errors.Op("pubsubqueue.pubsubConsumer.Done")
	casted, ok := msg.(*rawMessage)
	if !ok {
		return errors.E(op, "invalid message type")
	}
	casted.msg.Ack()
	return nil
}
func (p *pubsubConsumer) Failed(ctx context.Context, msg goduck.RawMessage) error {
	const op = errors.Op("pubsubqueue.pubsubConsumer.Failed")
	casted, ok := msg.(*rawMessage)
	if !ok {
		return errors.E(op, "invalid message type")
	}
	casted.msg.Nack()
	return nil
}
func (p *pubsubConsumer) Close() error {
	p.closeOnce.Do(func() {
		p.cancelFn()
		close(p.nextMessage)
		close(p.errChannel)
	})
	return nil
}
