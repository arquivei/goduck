package kafkaconfluent

type goduckMsg []byte

func (msg goduckMsg) Bytes() []byte {
	return msg
}

type topicPartition struct {
	topic     *string
	partition int32
}
