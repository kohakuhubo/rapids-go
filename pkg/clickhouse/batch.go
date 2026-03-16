package clickhouse

import (
	"berry-rapids-go/internal/clickhouse/client"
	"context"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
)

// BatchInserter 处理批量插入数据到 ClickHouse
type BatchInserter struct {
	client    *client.ClickHouseClient
	tableName string
	batchSize int
	buffer    *ColumnBuffer
}

// NewBatchInserter 创建一个新的批量插入器
func NewBatchInserter(chClient *client.ClickHouseClient, tableName string, batchSize int) *BatchInserter {
	return &BatchInserter{
		client:    chClient,
		tableName: tableName,
		batchSize: batchSize,
		buffer:    NewColumnBuffer(batchSize),
	}
}

// SetColumns 设置缓冲区的列名
func (bi *BatchInserter) SetColumns(columns []string) {
	bi.buffer.SetColumns(columns)
}

// AddRow 添加一行数据到批次中
func (bi *BatchInserter) AddRow(values []interface{}) error {
	if err := bi.buffer.Append(values); err != nil {
		return err
	}

	// 如果缓冲区已满，则刷新
	if bi.buffer.IsFull() {
		return bi.Flush(context.Background())
	}

	return nil
}

// Flush 将所有缓冲的数据插入到 ClickHouse
func (bi *BatchInserter) Flush(ctx context.Context) error {
	if bi.buffer.IsEmpty() {
		return nil
	}

	// 准备输入块
	input := make(proto.Input, len(bi.buffer.columns))
	for i, col := range bi.buffer.columns {
		input[i] = proto.InputColumn{
			Name: col,
			Data: bi.buffer.data[i],
		}
	}

	// 插入数据
	query := fmt.Sprintf("INSERT INTO %s VALUES", bi.tableName)
	err := bi.client.InsertBlock(ctx, query, input)
	if err != nil {
		return err
	}

	// 重置缓冲区
	bi.buffer.Reset()
	return nil
}
