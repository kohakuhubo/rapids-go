// block_data_event.go是Berry Rapids Go项目中区块数据事件的定义文件。
// 该文件定义了BlockDataEvent结构体，用于在聚合服务中传递数据块事件。
// 设计原理：提供统一的事件结构，支持不同类型数据块的封装和传递
// 实现方式：使用JSON序列化实现事件的序列化和反序列化，便于消息传递
package aggregate

import (
	"encoding/json"
	"time"
)

// BlockDataEvent区块数据事件结构体
// 设计说明：封装数据块事件的基本信息，支持JSON序列化
type BlockDataEvent struct {
	EventType string      `json:"eventType"`  // 事件类型
	Block     interface{} `json:"block"`      // 数据块内容
	Timestamp int64       `json:"timestamp"`  // 时间戳
}

// NewBlockDataEvent创建新的区块数据事件
// 设计原理：初始化区块数据事件实例，设置事件类型、数据块和时间戳
// 参数说明：eventType - 事件类型，block - 数据块内容
// 返回值：BlockDataEvent实例
func NewBlockDataEvent(eventType string, block interface{}) *BlockDataEvent {
	return &BlockDataEvent{
		EventType: eventType,                    // 设置事件类型
		Block:     block,                        // 设置数据块内容
		Timestamp: time.Now().UnixNano(),        // 设置当前时间戳
	}
}

// Type返回事件类型
// 设计原理：提供对事件类型的访问方法
// 返回值：事件类型字符串
func (e *BlockDataEvent) Type() string {
	return e.EventType
}

// Marshal序列化事件为字节数组
// 设计原理：将事件对象序列化为JSON格式的字节数组，便于网络传输
// 返回值：序列化后的字节数组
func (e *BlockDataEvent) Marshal() []byte {
	// 将事件对象序列化为JSON格式
	data, _ := json.Marshal(e)
	return data
}

// Unmarshal从字节数组反序列化事件
// 设计原理：将JSON格式的字节数组反序列化为事件对象
// 参数说明：data - 需要反序列化的字节数组
// 返回值：反序列化过程中可能的错误
func (e *BlockDataEvent) Unmarshal(data []byte) error {
	// 将字节数组反序列化为事件对象
	return json.Unmarshal(data, e)
}

// HasMessage检查事件是否包含有效消息
// 设计原理：判断数据块是否为空，用于事件有效性检查
// 返回值：如果数据块不为空则返回true，否则返回false
func (e *BlockDataEvent) HasMessage() bool {
	return e.Block != nil
}