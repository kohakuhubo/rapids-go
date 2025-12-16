// nats_registry.go是NATS JetStream数据源注册文件。
// 该文件负责将NATS JetStream数据源的创建函数注册到全局工厂中。
package nats

import (
	"berry-rapids-go/internal/data/source"
	"context"
)

// init函数在包初始化时自动注册NATS数据源创建函数
func init() {
	// 注册NATS数据源创建函数
	source.RegisterSourceCreator(source.NatsDataSourceType, createNatsSource)
}

// createNatsSource是NATS数据源的创建函数
// 该函数实现了source.SourceCreator函数签名，用于创建NATS数据源实例
// 参数说明：
//
//	ctx - 上下文，用于控制goroutine生命周期
//	config - NATS配置（应为*source.NatsConfig类型）
//
// 返回值：Source实例和可能的错误
func createNatsSource(ctx context.Context, config interface{}) (source.Source, error) {
	// 类型断言获取NATS配置
	natsConfig, ok := config.(*source.NatsConfig)
	if !ok {
		return nil, &ConfigError{Msg: "invalid NATS config type"}
	}

	// 创建并返回NATS数据源实例
	return NewNatsSource(ctx, natsConfig.URL, natsConfig.Subject, natsConfig.Stream)
}

// ConfigError表示配置错误
type ConfigError struct {
	Msg string
}

// Error返回错误信息
func (e *ConfigError) Error() string {
	return e.Msg
}
