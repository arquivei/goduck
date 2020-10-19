package kafkaconfluent

type goduckMsg struct {
	bytes    []byte
	metadata map[string][]byte
}

func (msg goduckMsg) Bytes() []byte {
	return msg.bytes
}

func (msg goduckMsg) Metadata() map[string][]byte {
	return msg.metadata
}

type topicPartition struct {
	topic     *string
	partition int32
}
