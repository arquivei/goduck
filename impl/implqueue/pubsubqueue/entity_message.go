package pubsubqueue

import "cloud.google.com/go/pubsub/v2"

type rawMessage struct {
	msg *pubsub.Message
}

func (r rawMessage) Bytes() []byte {
	return r.msg.Data
}
