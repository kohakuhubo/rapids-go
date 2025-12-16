package source

import (
	"bytes"
	"github.com/vmihailenco/msgpack/v5"
)

// MsgPackParser provides MessagePack parsing functionality
type MsgPackParser struct{}

// NewMsgPackParser creates a new MessagePack parser
func NewMsgPackParser() *MsgPackParser {
	return &MsgPackParser{}
}

// Parse parses MessagePack-encoded data into a map
func (p *MsgPackParser) Parse(data []byte) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := msgpack.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ParseInto parses MessagePack-encoded data into a specific struct
func (p *MsgPackParser) ParseInto(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}

// ParseRaw returns the raw MessagePack decoder which can be used for zero-copy access
func (p *MsgPackParser) ParseRaw(data []byte) (*msgpack.Decoder, error) {
	reader := bytes.NewReader(data)
	decoder := msgpack.NewDecoder(reader)
	return decoder, nil
}