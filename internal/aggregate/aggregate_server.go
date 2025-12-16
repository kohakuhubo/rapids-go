// aggregate_server.go是Berry Rapids Go项目中数据聚合服务的核心实现文件。
// 该文件定义了AggregateServer结构体，基于Watermill消息框架实现事件驱动的聚合计算。
// 设计原理：采用事件驱动架构，通过异步消息处理实现高性能的数据聚合计算
// 实现方式：使用Watermill库的发布/订阅模式，支持多种计算处理器的注册和路由
package aggregate

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"go.uber.org/zap"
)

// AggregateServiceHandler接口定义了聚合服务处理程序的基本接口
// 设计说明：提供统一的事件处理接口，便于扩展不同的聚合服务实现
type AggregateServiceHandler interface {
	// Handle处理上下文和区块数据事件
	Handle(ctx context.Context, event *BlockDataEvent) error
}

// CalculationHandler计算处理器接口定义了聚合计算的基本方法
// 设计说明：提供可扩展的计算处理器接口，支持不同类型的聚合计算
type CalculationHandler interface {
	// Type返回处理器处理的事件类型
	Type() string

	// Handle处理区块数据事件，返回聚合后的结果
	Handle(event *BlockDataEvent) interface{}
}

// AggregateServer 聚合服务主结构体
// 设计说明：基于Watermill消息框架实现事件驱动的聚合服务
type AggregateServer struct {
	logger    *zap.Logger                   // 日志记录器
	config    *Config                       // 聚合服务配置
	pubSub    *gochannel.GoChannel          // Watermill发布/订阅通道
	router    *message.Router               // Watermill消息路由器
	handlers  map[string]CalculationHandler // 计算处理器映射表
	publisher message.Publisher             // 消息发布器
}

// Config 聚合服务配置结构体
// 设计说明：定义聚合服务的核心配置参数
type Config struct {
	WaitTime    time.Duration // 等待时间，用于延迟处理
	QueueSize   int           // 队列大小，控制缓冲区容量
	WorkerCount int           // 工作线程数，控制并发处理能力
}

// NewAggregateServer 创建新的聚合服务实例
// 设计原理：初始化Watermill组件，配置发布/订阅通道和消息路由器
// 参数说明：logger - 日志记录器，config - 聚合服务配置
// 返回值：AggregateServer实例
func NewAggregateServer(logger *zap.Logger, config *Config) *AggregateServer {
	// 创建GoChannel发布/订阅通道，用于内部消息传递
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{
			OutputChannelBuffer: int64(config.QueueSize), // 设置输出通道缓冲区大小
		},
		watermill.NewStdLogger(false, false), // 创建标准日志记录器
	)

	// 创建消息路由器，用于路由消息到相应的处理器
	router, err := message.NewRouter(message.RouterConfig{}, watermill.NewStdLogger(false, false))
	if err != nil {
		logger.Fatal("Failed to create router", zap.Error(err))
	}

	// 返回聚合服务实例
	return &AggregateServer{
		logger:    logger,                              // 日志记录器
		config:    config,                              // 配置信息
		pubSub:    pubSub,                              // 发布/订阅通道
		router:    router,                              // 消息路由器
		handlers:  make(map[string]CalculationHandler), // 计算处理器映射表
		publisher: pubSub,                              // 消息发布器
	}
}

// RegisterHandler 注册计算处理器
// 设计原理：将计算处理器注册到映射表中，用于后续的消息路由
// 参数说明：handler - 需要注册的计算处理器
func (s *AggregateServer) RegisterHandler(handler CalculationHandler) {
	// 根据处理器的类型将其添加到映射表中
	s.handlers[handler.Type()] = handler
}

// Handle 处理区块数据事件
// 设计原理：将事件异步发布到事件总线，实现非阻塞处理
// 参数说明：ctx - 上下文，event - 区块数据事件
// 返回值：处理过程中可能的错误
func (s *AggregateServer) Handle(ctx context.Context, event *BlockDataEvent) error {
	// 在独立的goroutine中异步发布事件到事件总线
	go func() {
		// 创建Watermill消息实例
		msg := message.NewMessage(watermill.NewUUID(), event.Marshal())
		// 设置消息上下文
		msg.SetContext(ctx)

		// 等待指定时间后发布消息，实现延迟处理
		time.Sleep(s.config.WaitTime)

		// 发布消息到事件总线
		if err := s.publisher.Publish(event.Type(), msg); err != nil {
			// 如果发布失败，记录错误日志
			s.logger.Error("Failed to publish event",
				zap.String("type", event.Type()),
				zap.Error(err))
		}
	}()

	// 立即返回，不等待异步操作完成
	return nil
}

// Start 启动聚合服务
// 设计原理：配置消息路由规则并启动消息路由器
// 参数说明：ctx - 上下文
// 返回值：启动过程中可能的错误
func (s *AggregateServer) Start(ctx context.Context) error {
	// 为每个注册的处理器设置路由规则
	for eventType, handler := range s.handlers {
		handler := handler // 捕获循环变量，避免闭包问题
		// 添加消息处理路由规则
		s.router.AddHandler(
			"aggregate_"+eventType, // 处理器名称
			eventType,              // 输入主题
			s.pubSub,               // 输入发布/订阅通道
			eventType+"_result",    // 输出主题
			s.pubSub,               // 输出发布/订阅通道
			// 消息处理函数
			func(msg *message.Message) ([]*message.Message, error) {
				// 反序列化事件
				event := &BlockDataEvent{}
				if err := event.Unmarshal(msg.Payload); err != nil {
					s.logger.Error("Failed to unmarshal event",
						zap.String("type", eventType),
						zap.Error(err))
					return nil, nil
				}

				// 处理事件
				resultBlock := handler.Handle(event)
				if resultBlock == nil {
					return nil, nil
				}

				// 发布处理结果
				resultEvent := &BlockDataEvent{
					EventType: eventType + "_result", // 结果事件类型
					Block:     resultBlock,           // 处理结果
				}

				// 创建结果消息
				resultMsg := message.NewMessage(watermill.NewUUID(), resultEvent.Marshal())
				return []*message.Message{resultMsg}, nil
			},
		)
	}

	// 在独立的goroutine中启动路由器
	go func() {
		if err := s.router.Run(ctx); err != nil {
			s.logger.Error("Router stopped with error", zap.Error(err))
		}
	}()

	// 等待路由器启动完成
	<-s.router.Running()

	// 返回nil表示启动成功
	return nil
}

// Stop停止聚合服务
// 设计原理：优雅关闭消息路由器和发布/订阅通道
// 参数说明：ctx - 上下文
// 返回值：关闭过程中可能的错误
func (s *AggregateServer) Stop(ctx context.Context) error {
	// 关闭消息路由器
	s.router.Close()
	// 关闭发布/订阅通道
	return s.pubSub.Close()
}
