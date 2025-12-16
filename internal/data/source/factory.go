// factory.go 是Berry Rapids Go项目中数据源工厂的实现文件。
// 该文件提供了创建不同类型数据源的工厂方法。
// 设计原理：根据配置动态创建适当的数据源实例，实现数据源的可插拔性
// 实现方式：通过读取配置中的数据源类型，使用注册的创建函数创建对应的Source实例
package source

import (
	"context"
	"fmt"
)

// DataSourceType 定义数据源类型
type DataSourceType string

const (
	// KafkaDataSourceType 表示Kafka数据源
	KafkaDataSourceType DataSourceType = "kafka"
	
	// NatsDataSourceType 表示NATS JetStream数据源
	NatsDataSourceType DataSourceType = "nats"
)

// KafkaConfig 表示Kafka数据源配置
type KafkaConfig struct {
	Brokers []string `json:"brokers"` // Kafka集群地址列表
	Topic   string   `json:"topic"`   // 主题名称
}

// NatsConfig 表示NATS JetStream数据源配置
type NatsConfig struct {
	URL     string `json:"url"`     // NATS服务器地址
	Subject string `json:"subject"` // 主题名称
	Stream  string `json:"stream"`  // 流名称
}

// DataSourceConfig 表示数据源配置
type DataSourceConfig struct {
	Type  DataSourceType `json:"type"`  // 数据源类型
	Kafka *KafkaConfig   `json:"kafka,omitempty"` // Kafka配置
	Nats  *NatsConfig    `json:"nats,omitempty"`  // NATS配置
}

// SourceCreator 定义数据源创建函数的类型
type SourceCreator func(ctx context.Context, config interface{}) (Source, error)

// sourceCreators 存储不同类型数据源的创建函数
var sourceCreators = make(map[DataSourceType]SourceCreator)

// RegisterSourceCreator 注册数据源创建函数
// 该函数允许在运行时注册新的数据源类型及其创建函数
// 参数说明：
//   dataType - 数据源类型
//   creator - 数据源创建函数
func RegisterSourceCreator(dataType DataSourceType, creator SourceCreator) {
	sourceCreators[dataType] = creator
}

// CreateDataSource 根据配置创建数据源实例
// 设计原理：根据配置中的数据源类型，使用已注册的创建函数创建对应的Source实例
// 参数说明：
//   ctx - 上下文，用于控制goroutine生命周期
//   config - 数据源配置
// 返回值：Source实例和可能的错误
func CreateDataSource(ctx context.Context, config *DataSourceConfig) (Source, error) {
	// 获取对应数据源类型的创建函数
	creator, exists := sourceCreators[config.Type]
	if !exists {
		// 不支持的数据源类型
		return nil, fmt.Errorf("unsupported data source type: %s", config.Type)
	}
	
	// 根据数据源类型获取相应的配置
	var specificConfig interface{}
	switch config.Type {
	case KafkaDataSourceType:
		specificConfig = config.Kafka
	case NatsDataSourceType:
		specificConfig = config.Nats
	default:
		return nil, fmt.Errorf("unsupported data source type: %s", config.Type)
	}
	
	// 使用创建函数创建数据源实例
	return creator(ctx, specificConfig)
}
