// nats_source_entry.go是Berry Rapids Go项目中NATS数据条目的实现文件。
// 该文件定义了NatsSourceEntry 结构体，实现了SourceEntry接口，封装了NATS消息的访问方法。
// 设计原理：提供统一的数据条目接口，封装NATS消息的访问方法，便于上层应用使用
// 实现方式：通过NATS消息实例，提供GetKey、GetValue等方法访问消息内容
package nats

import (
	"berry-rapids-go/internal/data/source"

	"github.com/nats-io/nats.go/jetstream"
)

// NatsSourceEntry 表示来自NATS JetStream的单个数据条目
// 设计说明：该结构体封装了NATS JetStream消息，提供了统一的访问接口
type NatsSourceEntry struct {
	Msg    jetstream.Msg         // NATS消息
	Parser *source.MsgPackParser // MessagePack解析器
}

// NewNatsSourceEntry 创建一个新的NatsSourceEntry实例
func NewNatsSourceEntry(msg jetstream.Msg) *NatsSourceEntry {
	return &NatsSourceEntry{
		Msg:    msg,
		Parser: source.NewMsgPackParser(),
	}
}

// GetMessage 返回底层的NATS JetStream消息
// 设计原理：提供对底层消息对象的直接访问
// 返回值：NATS JetStream消息
func (nse *NatsSourceEntry) GetMessage() jetstream.Msg {
	// 返回底层NATS消息
	return nse.Msg
}

// GetKey 获取消息的键（或主题）
// 设计原理：提供对消息键或主题的访问
// 返回值：消息键或主题的字节切片
func (nse *NatsSourceEntry) GetKey() []byte {
	// NATS中使用主题作为键
	return []byte(nse.Msg.Subject())
}

// GetValue 获取消息的值（数据内容）
// 设计原理：提供对消息内容的访问
// 返回值：消息内容的字节切片
func (nse *NatsSourceEntry) GetValue() []byte {
	// 返回消息数据内容
	return nse.Msg.Data()
}

// GetParsedValue 解析并返回MessagePack格式的消息值
// 设计原理：提供对MessagePack编码消息的解析功能
// 返回值：解析后的数据和可能的错误
func (nse *NatsSourceEntry) GetParsedValue() (map[string]interface{}, error) {
	return nse.Parser.Parse(nse.Msg.Data())
}

// GetBinaryDataFromMsgPack 从MessagePack消息中提取二进制数据，实现零拷贝访问
// 设计原理：直接从原始字节数据中提取二进制字段，避免额外内存分配
// 参数：fieldName - 要提取的二进制字段名称
// 返回值：二进制数据的字节切片和可能的错误
func (nse *NatsSourceEntry) GetBinaryDataFromMsgPack(fieldName string) ([]byte, error) {
	// 获取原始MessagePack解码器
	decoder, err := nse.Parser.ParseRaw(nse.Msg.Data())
	if err != nil {
		return nil, err
	}

	// 解析Map并查找指定字段
	n, err := decoder.DecodeMapLen()
	if err != nil {
		return nil, err
	}

	for i := 0; i < n; i++ {
		// 解析键
		key, err := decoder.DecodeString()
		if err != nil {
			return nil, err
		}

		// 如果找到目标字段
		if key == fieldName {
			// 检查值类型是否为二进制数据
			kind, err := decoder.PeekCode()
			if err != nil {
				return nil, err
			}

			// 检查是否为二进制类型（通过类型码判断）
			// MessagePack二进制类型码范围: 0xc4-0xc6 (bin 8/16/32)
			// 字符串类型码范围: 0xa0-0xbf (fixstr), 0xd9-0xdb (str 8/16/32)
			switch {
			case kind >= 0xc4 && kind <= 0xc6: // 二进制类型
				return decoder.DecodeBytes()
			case kind >= 0xa0 && kind <= 0xbf: // 固定长度字符串
				str, err := decoder.DecodeBytes()
				if err != nil {
					return nil, err
				}
				return []byte(str), nil
			case kind >= 0xd9 && kind <= 0xdb: // 可变长度字符串
				str, err := decoder.DecodeString()
				if err != nil {
					return nil, err
				}
				return []byte(str), nil
			default:
				// 其他类型跳过
				decoder.Skip()
				continue
			}
		}

		// 跳过非目标字段的值
		decoder.Skip()
	}

	return nil, nil // 未找到指定字段
}

// GetSubject 获取消息的主题
// 设计原理：提供对消息主题的访问
// 返回值：消息主题字符串
func (nse *NatsSourceEntry) GetSubject() string {
	// 返回消息主题
	return nse.Msg.Subject()
}

// Ack 确认消息处理完成
// 设计原理：告知NATS JetStream消息已被成功处理
// 返回值：确认过程中可能的错误
func (nse *NatsSourceEntry) Ack() error {
	// 确认消息处理完成
	return nse.Msg.Ack()
}

// Nack 标记消息为未处理
// 设计原理：告知NATS JetStream消息处理失败
// 返回值：标记过程中可能的错误
func (nse *NatsSourceEntry) Nack() error {
	// 标记消息为未处理
	return nse.Msg.Nak()
}

// GetConsumer获取消息的消费者名称
// 设计原理：提供对消息消费者信息的访问
// 返回值：消费者名称字符串
func (nse *NatsSourceEntry) GetConsumer() string {
	// 返回消费者名称
	metadata, err := nse.Msg.Metadata()
	if err != nil {
		return ""
	}
	return metadata.Consumer
}

func (nse *NatsSourceEntry) GetRowId() int64 {
	metadata, err := nse.Msg.Metadata()
	if err != nil {
		return -1
	}
	return int64(metadata.Sequence.Stream)
}
