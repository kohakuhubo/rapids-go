// consumer.go 是Berry Rapids Go项目中Kafka消费者实现的核心文件。
// 该文件定义了Consumer 结构体，负责从Kafka集群读取数据。
// 设计原理：采用分区并行消费模式，每个分区创建独立的消费者，通过goroutine并发处理
// 实现方式：使用Sarama客户端库，通过通道进行并发数据传递，提供异步消费能力
package kafka

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
)

// Consumer 表示一个Sarama消费者实例
// 设计说明：该结构体封装了底层的Sarama消费者，提供了更高层的抽象
type Consumer struct {
	Consumer   sarama.Consumer // 底层Sarama消费者实例
	Topics     []string        // 订阅的主题列表
	Partitions []int32         // 主题的所有分区
	ctx        context.Context // 上下文，用于控制goroutine生命周期
}

// NewConsumer 创建一个新的Sarama消费者
// 设计原理：初始化消费者配置，获取主题分区信息，为并行消费做准备
// 参数说明：
//   ctx - 上下文，用于控制goroutine生命周期
//   brokers - Kafka集群地址列表
//   topics - 需要消费的主题列表
// 返回值：消费者实例和错误信息
func NewConsumer(ctx context.Context, brokers []string, topics []string) (*Consumer, error) {
	// 创建Sarama消费者配置
	config := sarama.NewConfig()
	// 配置消费者返回错误信息，便于错误处理
	config.Consumer.Return.Errors = true

	// 创建Sarama消费者实例
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}

	// 收集所有主题的分区信息
	partitions := make([]int32, 0)
	for _, topic := range topics {
		tp, err := consumer.Partitions(topic)
		if err != nil {
			return nil, err
		}
		// 将所有分区添加到分区列表
		partitions = append(partitions, tp...)
	}

	// 返回消费者实例
	return &Consumer{
		Consumer:   consumer,   // Sarama消费者实例
		Topics:     topics,     // 主题列表
		Partitions: partitions, // 分区列表
		ctx:        ctx,        // 上下文
	}, nil
}

// ConsumeMessages 启动从指定主题的消息消费
// 设计原理：为每个分区启动独立的goroutine，实现并行消费
// 实现方式：通过通道传递消息和错误，支持异步处理
// 参数说明：topic - 需要消费的主题
// 返回值：消息通道和错误通道，用于异步接收数据
func (c *Consumer) ConsumeMessages(topic string) (<-chan *sarama.ConsumerMessage, <-chan error) {
	// 创建消息通道，缓冲区大小为256，提高并发性能
	messageChan := make(chan *sarama.ConsumerMessage, 256)
	// 创建错误通道，缓冲区大小为16，避免错误消息丢失
	errorChan := make(chan error, 16)

	// 获取指定主题的所有分区
	partitionList, err := c.Consumer.Partitions(topic)
	if err != nil {
		// 如果获取分区失败，将错误发送到错误通道并关闭通道
		errorChan <- err
		close(messageChan)
		close(errorChan)
		return messageChan, errorChan
	}

	// 为每个分区启动独立的消费者goroutine，实现并行处理
	for _, partition := range partitionList {
		// 从最新偏移量开始消费消息
		pc, err := c.Consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			// 如果分区消费启动失败，发送错误到错误通道
			errorChan <- fmt.Errorf("failed to start consumer for partition %d: %v", partition, err)
			continue
		}

		// 为每个分区消费者启动独立的goroutine
		go func(partitionConsumer sarama.PartitionConsumer) {
			// 在goroutine结束时异步关闭分区消费者
			defer partitionConsumer.AsyncClose()

			// 无限循环处理分区消息，同时监听上下文取消信号
			for {
				select {
				case <-c.ctx.Done():
					// 收到上下文取消信号，退出循环
					return
				case msg, ok := <-partitionConsumer.Messages():
					// 检查通道是否已关闭
					if !ok {
						return
					}
					// 从分区消费者接收消息并发送到消息通道
					messageChan <- msg
				case err, ok := <-partitionConsumer.Errors():
					// 检查错误通道是否已关闭
					if !ok {
						return
					}
					// 从分区消费者接收错误并发送到错误通道
					errorChan <- err
				}
			}
		}(pc)
	}

	// 返回消息通道和错误通道，供调用者异步接收数据
	return messageChan, errorChan
}

// Close 关闭消费者并释放相关资源
// 设计原理：确保消费者正确关闭，避免资源泄漏
// 实现方式：调用底层Sarama消费者的Close方法
// 返回值：关闭过程中可能的错误
func (c *Consumer) Close() error {
	// 调用底层Sarama消费者的Close方法关闭消费者
	return c.Consumer.Close()
}