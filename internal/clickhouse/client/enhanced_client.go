// enhanced_client.go是Berry Rapids Go项目中增强版ClickHouse客户端的实现文件。
// 该文件定义了EnhancedClickHouseClient结构体，扩展了基础ClickHouse客户端的功能，
// 提供了批处理和重试机制，增强数据写入的可靠性和性能。
// 设计原理：在基础客户端之上添加批处理和重试功能，提高数据写入的稳定性和效率
// 实现方式：封装基础客户端，提供批处理插入方法和指数退避重试机制
package client

import (
	"berry-rapids-go/internal/model"
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// EnhancedClickHouseClient扩展了基础ClickHouse客户端，增加了批处理能力
// 设计说明：通过组合基础客户端，提供更高级的数据插入功能
type EnhancedClickHouseClient struct {
	*ClickHouseClient  // 嵌入基础ClickHouse客户端
	logger *zap.Logger  // 日志记录器
}

// NewEnhancedClickHouseClient创建一个新的增强版ClickHouse客户端
// 设计原理：初始化增强客户端，组合基础客户端和日志记录器
// 参数说明：client - 基础ClickHouse客户端，logger - 日志记录器
// 返回值：EnhancedClickHouseClient实例
func NewEnhancedClickHouseClient(client *ClickHouseClient, logger *zap.Logger) *EnhancedClickHouseClient {
	return &EnhancedClickHouseClient{
		ClickHouseClient: client,  // 基础客户端实例
		logger:           logger,  // 日志记录器
	}
}

// InsertBatch将一批数据插入到ClickHouse中
// 设计原理：提供简化的批处理插入接口，记录插入操作的日志信息
// 参数说明：ctx - 上下文，tableName - 目标表名，batch - 批数据
// 返回值：插入过程中可能的错误
func (c *EnhancedClickHouseClient) InsertBatch(ctx context.Context, tableName string, batch *model.BatchData) error {
	// 这是一个示例实现，假设批数据内容已经正确格式化
	// 在实际实现中，您需要解析批数据内容并将其转换为适当的格式

	// 这是一个简化的示例 - 在实践中，您需要：
	// 1. 解析批数据内容
	// 2. 将其转换为proto.Input格式
	// 3. 执行插入操作

	// 记录插入操作的日志信息
	c.logger.Info("Inserting batch into ClickHouse",
		zap.String("table", tableName),           // 目标表名
		zap.Int64("startRowID", batch.StartRowID), // 起始行ID
		zap.Int64("endRowID", batch.EndRowID),     // 结束行ID
		zap.Int64("sizeInBytes", batch.SizeInBytes), // 数据大小（字节）
		zap.Int64("numberOfRows", batch.NumberOfRows)) // 行数

	// 在实际实现中，您会使用客户端的InsertBlock方法
	// 现在我们只是记录会插入数据
	// 您需要将batch.Content转换为proto.Input格式

	return nil
}

// InsertBatchWithRetry将一批数据插入到ClickHouse中，并带有重试逻辑
// 设计原理：通过指数退避算法实现重试机制，提高数据插入的可靠性
// 参数说明：ctx - 上下文，tableName - 目标表名，batch - 批数据，maxRetries - 最大重试次数
// 返回值：插入过程中可能的错误
func (c *EnhancedClickHouseClient) InsertBatchWithRetry(ctx context.Context, tableName string, batch *model.BatchData, maxRetries int) error {
	var lastErr error

	// 循环尝试插入，最多重试maxRetries次
	for i := 0; i <= maxRetries; i++ {
		// 尝试插入批数据
		if err := c.InsertBatch(ctx, tableName, batch); err != nil {
			// 记录错误和重试信息
			lastErr = err
			c.logger.Warn("Failed to insert batch, retrying...",
				zap.Int("attempt", i+1),      // 当前尝试次数
				zap.Int("maxRetries", maxRetries), // 最大重试次数
				zap.Error(err))               // 错误信息

			// 如果不是最后一次尝试，则等待后重试
			if i < maxRetries {
				// 指数退避：等待时间随重试次数增加而增加
				time.Sleep(time.Duration(1<<i) * 100 * time.Millisecond)
				continue
			}

			// 达到最大重试次数后返回错误
			return fmt.Errorf("failed to insert batch after %d retries: %w", maxRetries, err)
		}

		// 插入成功，直接返回
		return nil
	}

	// 返回最后一次错误
	return lastErr
}
