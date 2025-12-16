// kafka_source.go 是Berry Rapids Go项目中Kafka数据源的实现文件。
// 该文件定义了KafkaSource 结构体，实现了Source接口，负责与底层Kafka消费者交互。
// 设计原理：提供统一的数据源接口，封装Kafka消费者的复杂性，便于上层应用使用
// 实现方式：通过组合Consumer结构体，提供Next方法获取数据，Close方法释放资源
package kafka

import (
	"berry-rapids-go/internal/data/source"
	"context"
)

// KafkaSource 实现了Source接口，用于从Kafka读取数据
// 设计说明：该结构体作为Kafka消费者的包装器，提供简化的数据访问接口
type KafkaSource struct {
	Consumer *Consumer // 底层Kafka消费者实例
	Topic    string    // 消费的主题名称
}

// NewKafkaSource 创建一个新的KafkaSource实例
// 设计原理：初始化Kafka消费者并建立与指定主题的连接
// 参数说明：
//
//	ctx - 上下文，用于控制goroutine生命周期
//	brokers - Kafka集群地址列表
//	topic - 需要消费的主题名称
//
// 返回值：KafkaSource实例和可能的错误
func NewKafkaSource(ctx context.Context, brokers []string, topic string) (*KafkaSource, error) {
	// 创建Kafka消费者实例，订阅指定主题
	consumer, err := NewConsumer(ctx, brokers, []string{topic})
	if err != nil {
		return nil, err
	}

	// 返回KafkaSource实例
	return &KafkaSource{
		Consumer: consumer, // Kafka消费者实例
		Topic:    topic,    // 主题名称
	}, nil
}

// Next 从Kafka获取下一条消息
// 设计原理：通过调用底层消费者获取消息，封装为SourceEntry接口返回
// 实现方式：使用通道接收消息，通过select语句处理消息和上下文取消
// 返回值：SourceEntry接口实例和可能的错误
func (ks *KafkaSource) Next() (source.SourceEntry, error) {
	// 启动消息消费，获取消息通道和错误通道
	// 这是一个简化的实现，在生产系统中，您可能需要获取一批消息并正确处理偏移量提交
	messageChan, errorChan := ks.Consumer.ConsumeMessages(ks.Topic)

	// 使用select语句等待消息或错误，同时监听上下文取消
	select {
	case msg := <-messageChan:
		// 从消息通道接收到消息，创建KafkaSourceEntry实例
		entry := NewKafkaSourceEntry(msg) // 使用新的构造函数
		return entry, nil // 返回SourceEntry实例
	case err := <-errorChan:
		// 从错误通道接收到错误，直接返回错误
		return nil, err // 返回错误
	case <-ks.Consumer.ctx.Done():
		// 收到上下文取消信号，返回上下文错误
		return nil, ks.Consumer.ctx.Err()
	}
}

// Close 关闭Kafka消费者并释放相关资源
// 设计原理：确保消费者正确关闭，避免资源泄漏
// 实现方式：调用底层Consumer的Close方法
// 返回值：关闭过程中可能的错误
func (ks *KafkaSource) Close() error {
	// 调用底层Consumer的Close方法关闭消费者
	return ks.Consumer.Close()
}
