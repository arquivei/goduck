package kafkasarama

type rawMessage []byte

func (r rawMessage) Bytes() []byte {
	return r
}
