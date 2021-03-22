package pubsubqueue

import "cloud.google.com/go/pubsub"

type rawMessage struct {
	msg *pubsub.Message
}

func (r rawMessage) Bytes() []byte {
	return r.msg.Data
}
