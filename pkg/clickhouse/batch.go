package clickhouse

import (
	"berry-rapids-go/internal/clickhouse/client"
	"context"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
)

// BatchInserter handles batch insertion of data into ClickHouse
type BatchInserter struct {
	client    *client.ClickHouseClient
	tableName string
	batchSize int
	buffer    *ColumnBuffer
}

// NewBatchInserter creates a new batch inserter
func NewBatchInserter(chClient *client.ClickHouseClient, tableName string, batchSize int) *BatchInserter {
	return &BatchInserter{
		client:    chClient,
		tableName: tableName,
		batchSize: batchSize,
		buffer:    NewColumnBuffer(batchSize),
	}
}

// SetColumns sets the column names for the buffer
func (bi *BatchInserter) SetColumns(columns []string) {
	bi.buffer.SetColumns(columns)
}

// AddRow adds a row of data to the batch
func (bi *BatchInserter) AddRow(values []interface{}) error {
	if err := bi.buffer.Append(values); err != nil {
		return err
	}

	// If buffer is full, flush it
	if bi.buffer.IsFull() {
		return bi.Flush(context.Background())
	}

	return nil
}

// Flush inserts all buffered data into ClickHouse
func (bi *BatchInserter) Flush(ctx context.Context) error {
	if bi.buffer.IsEmpty() {
		return nil
	}

	// Prepare the input block
	input := make(proto.Input, len(bi.buffer.columns))
	for i, col := range bi.buffer.columns {
		input[i] = proto.InputColumn{
			Name: col,
			Data: bi.buffer.data[i],
		}
	}

	// Insert the data
	query := fmt.Sprintf("INSERT INTO %s VALUES", bi.tableName)
	err := bi.client.InsertBlock(ctx, query, input)
	if err != nil {
		return err
	}

	// Reset the buffer
	bi.buffer.Reset()
	return nil
}