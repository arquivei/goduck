package implqueue

import "cloud.google.com/go/pubsub"

type rawMessage struct {
	msg *pubsub.Message
}

func (r rawMessage) Bytes() []byte {
	return r.msg.Data
}

type mockRawMessage struct {
	data []byte
	idx  int
}

func (m mockRawMessage) Bytes() []byte {
	return m.data
}
