package consistency

import (
	"berry-rapids-go/internal/aggregate"
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"
)

// DefaultAggregatePersistenceHandler 默认聚合持久化处理器
type DefaultAggregatePersistenceHandler struct {
	client clickhouse.Conn
	logger *zap.Logger
	config interface{}
}

// NewDefaultAggregatePersistenceHandler 创建默认聚合持久化处理器
func NewDefaultAggregatePersistenceHandler(
	client clickhouse.Conn,
	config interface{},
	logger *zap.Logger,
) *DefaultAggregatePersistenceHandler {
	return &DefaultAggregatePersistenceHandler{
		client: client,
		config: config,
		logger: logger,
	}
}

// Handle 处理聚合数据并写入ClickHouse
func (h *DefaultAggregatePersistenceHandler) Handle(event *aggregate.BlockDataEvent) bool {
	if event == nil || !event.HasMessage() {
		return false
	}

	// 将事件数据转换为ClickHouse可以处理的格式
	blockData, ok := event.Block.(map[string]interface{})
	if !ok {
		h.logger.Error("Invalid block data format")
		return false
	}

	// 根据事件类型确定目标表
	tableName := event.EventType

	// 准备插入语句
	insertQuery := h.buildInsertQuery(tableName, blockData)

	// 执行插入操作
	ctx := context.Background()
	batch, err := h.client.PrepareBatch(ctx, insertQuery)
	if err != nil {
		h.logger.Error("Failed to prepare batch", zap.Error(err))
		return false
	}

	// 添加数据到批次
	if err := h.addDataToBatch(batch, blockData); err != nil {
		h.logger.Error("Failed to add data to batch", zap.Error(err))
		return false
	}

	// 提交批次
	if err := batch.Send(); err != nil {
		h.logger.Error("Failed to send batch", zap.Error(err))
		return false
	}

	h.logger.Info("Successfully persisted aggregated data",
		zap.String("table", tableName),
		zap.Int("rows", 1))

	return true
}

// buildInsertQuery 构建插入查询语句
func (h *DefaultAggregatePersistenceHandler) buildInsertQuery(tableName string, data map[string]interface{}) string {
	// 构建INSERT语句，这里需要根据实际的表结构进行调整
	columns := ""
	values := ""

	first := true
	for key := range data {
		if !first {
			columns += ", "
			values += ", "
		}
		columns += key
		values += "?"
		first = false
	}

	return "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + values + ")"
}

// addDataToBatch 将数据添加到批次中
func (h *DefaultAggregatePersistenceHandler) addDataToBatch(batch driver.Batch, data map[string]interface{}) error {
	values := make([]interface{}, 0, len(data))

	for _, value := range data {
		values = append(values, value)
	}

	return batch.Append(values...)
}

// Start 启动处理器
func (h *DefaultAggregatePersistenceHandler) Start() error {
	// 如果需要额外的初始化逻辑，可以在这里添加
	h.logger.Info("DefaultAggregatePersistenceHandler started")
	return nil
}

// Stop 停止处理器
func (h *DefaultAggregatePersistenceHandler) Stop() error {
	// 如果需要清理逻辑，可以在这里添加
	h.logger.Info("DefaultAggregatePersistenceHandler stopped")
	return nil
}
