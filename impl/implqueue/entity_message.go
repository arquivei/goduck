package implqueue

import "cloud.google.com/go/pubsub"

type rawMessage struct {
	msg      *pubsub.Message
	metadata map[string][]byte
}

func (r rawMessage) Bytes() []byte {
	return r.msg.Data
}

func (r rawMessage) Metadata() map[string][]byte {
	return r.metadata
}
