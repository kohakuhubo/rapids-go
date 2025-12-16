// nats_source.go 是Berry Rapids Go项目中NATS JetStream数据源的实现文件。
// 该文件定义了NatsSource 结构体，实现了Source接口，负责与NATS JetStream交互。
// 设计原理：提供统一的数据源接口，封装NATS JetStream的复杂性，便于上层应用使用
// 实现方式：通过NATS JetStream客户端，提供Next方法获取数据，Close方法释放资源
package nats

import (
	"berry-rapids-go/internal/data/source"
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NatsSource 实现了Source接口，用于从NATS JetStream读取数据
// 设计说明：该结构体作为NATS JetStream消费者的包装器，提供简化的数据访问接口
type NatsSource struct {
	Conn      *nats.Conn          // NATS连接
	JetStream jetstream.JetStream // JetStream实例
	Consumer  jetstream.Consumer  // JetStream消费者
	Subject   string              // 消费的主题名称
	Stream    string              // 流名称
	ctx       context.Context     // 上下文，用于控制goroutine生命周期
	cancel    context.CancelFunc  // 取消函数，用于优雅关闭
}

// NewNatsSource 创建一个新的NatsSource实例
// 设计原理：初始化NATS连接和JetStream消费者并建立与指定主题的连接
// 参数说明：
//
//	ctx - 上下文，用于控制goroutine生命周期
//	url - NATS服务器地址
//	subject - 需要消费的主题名称
//	stream - 流名称
//
// 返回值：NatsSource实例和可能的错误
func NewNatsSource(ctx context.Context, url, subject, stream string) (*NatsSource, error) {
	// 创建NATS连接
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// 创建JetStream上下文
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	// 确保流存在，如果不存在则创建
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     stream,
		Subjects: []string{subject},
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create or update stream: %w", err)
	}

	// 创建消费者
	consumer, err := js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable:       "berry-rapids-consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 1024,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	// 创建可取消的上下文
	ctx, cancel := context.WithCancel(ctx)

	// 返回NatsSource实例
	return &NatsSource{
		Conn:      nc,       // NATS连接
		JetStream: js,       // JetStream实例
		Consumer:  consumer, // JetStream消费者
		Subject:   subject,  // 主题名称
		Stream:    stream,   // 流名称
		ctx:       ctx,      // 上下文
		cancel:    cancel,   // 取消函数
	}, nil
}

// Next 从NATS JetStream获取下一条消息
// 设计原理：通过调用底层消费者获取消息，封装为SourceEntry接口返回
// 实现方式：使用Next方法获取消息，通过select语句处理消息和上下文取消
// 返回值：SourceEntry接口实例和可能的错误
func (ns *NatsSource) Next() (source.SourceEntry, error) {
	// 启动消息消费，获取消息通道和错误通道
	// 这是一个简化的实现，在生产系统中，您可能需要获取一批消息并正确处理确认
	msg, err := ns.Consumer.Next()
	if err != nil {
		// 如果是上下文取消错误，返回上下文错误
		select {
		case <-ns.ctx.Done():
			return nil, ns.ctx.Err()
		default:
			return nil, err
		}
	}

	// 创建NatsSourceEntry实例
	entry := NewNatsSourceEntry(msg) // 使用新的构造函数

	// 返回SourceEntry实例
	return entry, nil
}

// Close 关闭NATS连接并释放相关资源
// 设计原理：确保消费者正确关闭，避免资源泄漏
// 实现方式：调用取消函数，然后关闭NATS连接
// 返回值：关闭过程中可能的错误
func (ns *NatsSource) Close() error {
	// 调用取消函数
	if ns.cancel != nil {
		ns.cancel()
	}

	// 关闭NATS连接
	if ns.Conn != nil {
		ns.Conn.Close()
	}

	// 返回nil表示关闭成功
	return nil
}
