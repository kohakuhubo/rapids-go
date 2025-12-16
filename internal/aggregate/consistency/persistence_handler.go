package consistency

import (
	"berry-rapids-go/internal/aggregate"
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/zap"
)

// AggregateBlockPersistenceHandler 聚合块持久化处理器
type AggregateBlockPersistenceHandler struct {
	logger           *zap.Logger
	clickHouseClient clickhouse.Conn
	config           interface{} // 这里可以使用具体的配置结构
	handlers         map[string]AggregatePersistenceHandler
}

// AggregatePersistenceHandler 聚合持久化处理器接口
type AggregatePersistenceHandler interface {
	// Handle 处理聚合数据并持久化
	Handle(event *aggregate.BlockDataEvent) bool
}

// NewAggregateBlockPersistenceHandler 创建新的聚合块持久化处理器
func NewAggregateBlockPersistenceHandler(
	client clickhouse.Conn,
	config interface{},
	logger *zap.Logger,
) *AggregateBlockPersistenceHandler {
	return &AggregateBlockPersistenceHandler{
		logger:           logger,
		clickHouseClient: client,
		config:           config,
		handlers:         make(map[string]AggregatePersistenceHandler),
	}
}

// RegisterHandler 注册特定类型的持久化处理器
func (h *AggregateBlockPersistenceHandler) RegisterHandler(
	sourceType string,
	handler AggregatePersistenceHandler,
) {
	h.handlers[sourceType] = handler
}

// Handle 处理区块数据事件
func (h *AggregateBlockPersistenceHandler) Handle(ctx context.Context, event *aggregate.BlockDataEvent) bool {
	if event == nil {
		return false
	}

	// 根据事件类型选择对应的处理器
	if handler, exists := h.handlers[event.EventType]; exists {
		return handler.Handle(event)
	}

	// 如果没有找到特定类型的处理器，使用默认处理器
	defaultHandler := &DefaultAggregatePersistenceHandler{
		client: h.clickHouseClient,
		logger: h.logger,
	}
	return defaultHandler.Handle(event)
}

// Start 启动持久化处理器
func (h *AggregateBlockPersistenceHandler) Start(ctx context.Context) error {
	// 启动所有注册的处理器
	for _, handler := range h.handlers {
		if startable, ok := handler.(interface{ Start() error }); ok {
			if err := startable.Start(); err != nil {
				h.logger.Error("Failed to start handler", zap.Error(err))
			}
		}
	}
	return nil
}

// Stop 停止持久化处理器
func (h *AggregateBlockPersistenceHandler) Stop(ctx context.Context) error {
	// 停止所有注册的处理器
	for _, handler := range h.handlers {
		if stoppable, ok := handler.(interface{ Stop() error }); ok {
			if err := stoppable.Stop(); err != nil {
				h.logger.Error("Failed to stop handler", zap.Error(err))
			}
		}
	}
	return nil
}
