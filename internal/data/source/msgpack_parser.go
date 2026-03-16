package source

import (
	"bytes"
	"github.com/vmihailenco/msgpack/v5"
)

// MsgPackParser 提供 MessagePack 解析功能
type MsgPackParser struct{}

// NewMsgPackParser 创建一个新的 MessagePack 解析器
func NewMsgPackParser() *MsgPackParser {
	return &MsgPackParser{}
}

// Parse 将 MessagePack 编码的数据解析为 map
func (p *MsgPackParser) Parse(data []byte) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := msgpack.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ParseInto 将 MessagePack 编码的数据解析为指定的结构体
func (p *MsgPackParser) ParseInto(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}

// ParseRaw 返回原始的 MessagePack 解码器，可用于零拷贝访问
func (p *MsgPackParser) ParseRaw(data []byte) (*msgpack.Decoder, error) {
	reader := bytes.NewReader(data)
	decoder := msgpack.NewDecoder(reader)
	return decoder, nil
}
