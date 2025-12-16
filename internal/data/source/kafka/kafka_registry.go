// kafka_registry.go是Kafka数据源注册文件。
// 该文件负责将Kafka数据源的创建函数注册到全局工厂中。
package kafka

import (
	"berry-rapids-go/internal/data/source"
	"context"
)

// init函数在包初始化时自动注册Kafka数据源创建函数
func init() {
	// 注册Kafka数据源创建函数
	source.RegisterSourceCreator(source.KafkaDataSourceType, createKafkaSource)
}

// createKafkaSource是Kafka数据源的创建函数
// 该函数实现了source.SourceCreator函数签名，用于创建Kafka数据源实例
// 参数说明：
//
//	ctx - 上下文，用于控制goroutine生命周期
//	config - Kafka配置（应为*source.KafkaConfig类型）
//
// 返回值：Source实例和可能的错误
func createKafkaSource(ctx context.Context, config interface{}) (source.Source, error) {
	// 类型断言获取Kafka配置
	kafkaConfig, ok := config.(*source.KafkaConfig)
	if !ok {
		return nil, &ConfigError{Msg: "invalid Kafka config type"}
	}

	// 创建并返回Kafka数据源实例
	return NewKafkaSource(ctx, kafkaConfig.Brokers, kafkaConfig.Topic)
}

// ConfigError表示配置错误
type ConfigError struct {
	Msg string
}

// Error返回错误信息
func (e *ConfigError) Error() string {
	return e.Msg
}
