package implqueue

type mockRawMessage struct {
	data []byte
	idx  int
}

func (m mockRawMessage) Bytes() []byte {
	return m.data
}

func (m mockRawMessage) Metadata() map[string][]byte {
	return nil
}
