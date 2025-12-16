// kafka_source_entry.go 是Berry Rapids Go项目中Kafka数据条目的实现文件。
// 该文件定义了KafkaSourceEntry 结构体，实现了SourceEntry接口，封装了Kafka消息的访问方法。
// 设计原理：提供统一的数据条目接口，封装Kafka消息的复杂性，便于上层应用访问消息内容
// 实现方式：通过组合sarama.ConsumerMessage结构体，提供类型安全的消息访问方法
package kafka

import (
	"berry-rapids-go/internal/data/source"

	"github.com/IBM/sarama"
)

// KafkaSourceEntry 表示来自Kafka的单个数据条目
// 设计说明：该结构体封装了底层的Kafka消费者消息，提供简化的访问接口
type KafkaSourceEntry struct {
	Message *sarama.ConsumerMessage // 底层Kafka消费者消息
	Parser  *source.MsgPackParser   // MessagePack解析器
}

// NewKafkaSourceEntry 创建一个新的KafkaSourceEntry实例
func NewKafkaSourceEntry(message *sarama.ConsumerMessage) *KafkaSourceEntry {
	return &KafkaSourceEntry{
		Message: message,
		Parser:  source.NewMsgPackParser(),
	}
}

// GetMessage 返回底层的Kafka消息
// 设计原理：提供对原始Kafka消息的直接访问
// 返回值：sarama.ConsumerMessage实例
func (kse *KafkaSourceEntry) GetMessage() *sarama.ConsumerMessage {
	return kse.Message
}

// GetKey 返回消息的键
// 设计原理：提供对消息键的类型安全访问
// 返回值：消息键的字节切片
func (kse *KafkaSourceEntry) GetKey() []byte {
	return kse.Message.Key
}

// GetValue 返回消息的值
// 设计原理：提供对消息值的类型安全访问
// 返回值：消息值的字节切片
func (kse *KafkaSourceEntry) GetValue() []byte {
	return kse.Message.Value
}

// GetParsedValue 解析并返回MessagePack格式的消息值
// 设计原理：提供对MessagePack编码消息的解析功能
// 返回值：解析后的数据和可能的错误
func (kse *KafkaSourceEntry) GetParsedValue() (map[string]interface{}, error) {
	return kse.Parser.Parse(kse.Message.Value)
}

// GetBinaryDataFromMsgPack 从MessagePack消息中提取二进制数据，实现零拷贝访问
// 设计原理：直接从原始字节数据中提取二进制字段，避免额外内存分配
// 参数：fieldName - 要提取的二进制字段名称
// 返回值：二进制数据的字节切片和可能的错误
func (kse *KafkaSourceEntry) GetBinaryDataFromMsgPack(fieldName string) ([]byte, error) {
	// 获取原始MessagePack解码器
	decoder, err := kse.Parser.ParseRaw(kse.Message.Value)
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
				str, err := decoder.DecodeString()
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

// GetTopic 返回消息的主题名称
// 设计原理：提供对消息主题的类型安全访问
// 返回值：主题名称字符串
func (kse *KafkaSourceEntry) GetTopic() string {
	return kse.Message.Topic
}

// GetPartition 返回消息的分区号
// 设计原理：提供对消息分区的类型安全访问
// 返回值：分区号
func (kse *KafkaSourceEntry) GetPartition() int32 {
	return kse.Message.Partition
}

// GetRowId 返回消息的偏移量
// 设计原理：提供对消息偏移量的类型安全访问
// 返回值：偏移量
func (kse *KafkaSourceEntry) GetRowId() int64 {
	return kse.Message.Offset
}
