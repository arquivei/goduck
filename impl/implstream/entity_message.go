package implstream

type rawMessage struct {
	bytes    []byte
	metadata map[string][]byte
}

func (r rawMessage) Bytes() []byte {
	return r.bytes
}
func (r rawMessage) Metadata() map[string][]byte {
	return r.metadata
}
