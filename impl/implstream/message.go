package implstream

type rawMessage []byte

func (r rawMessage) Bytes() []byte {
	return r
}

type mockRawMessage struct {
	data []byte
	idx  int
}

func (m mockRawMessage) Bytes() []byte {
	return m.data
}
