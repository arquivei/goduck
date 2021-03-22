package implstream

type mockRawMessage struct {
	data []byte
	idx  int
}

func (m mockRawMessage) Bytes() []byte {
	return m.data
}
