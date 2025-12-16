package clickhouse

import (
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// ColumnBuffer holds buffered column data for batch insertion
type ColumnBuffer struct {
	columns     []string
	data        []proto.ColInput
	currentSize int
	batchSize   int
}

// NewColumnBuffer creates a new column buffer
func NewColumnBuffer(batchSize int) *ColumnBuffer {
	return &ColumnBuffer{
		batchSize: batchSize,
	}
}

// SetColumns sets the column names and initializes the data columns
func (cb *ColumnBuffer) SetColumns(columns []string) {
	cb.columns = columns
	cb.data = make([]proto.ColInput, len(columns))

	// Initialize column data based on expected types
	// In a real implementation, you would determine types from schema
	for i := range cb.data {
		// Default to string columns for this example
		cb.data[i] = &proto.ColStr{}
	}
}

// Append appends a row of data to the buffer
func (cb *ColumnBuffer) Append(values []interface{}) error {
	if len(values) != len(cb.data) {
		return fmt.Errorf("values and columns length mismatch: %d != %d", len(values), len(cb.data))
	}

	for i, val := range values {
		switch v := val.(type) {
		case uint64:
			if col, ok := cb.data[i].(*proto.ColUInt64); ok {
				col.Append(v)
			} else {
				// Convert column type if necessary
				cb.data[i] = &proto.ColUInt64{}
				cb.data[i].(*proto.ColUInt64).Append(v)
			}
		case string:
			if col, ok := cb.data[i].(*proto.ColStr); ok {
				col.Append(v)
			} else {
				// Convert column type if necessary
				cb.data[i] = &proto.ColStr{}
				cb.data[i].(*proto.ColStr).Append(v)
			}
		case float64:
			if col, ok := cb.data[i].(*proto.ColFloat64); ok {
				col.Append(v)
			} else {
				// Convert column type if necessary
				cb.data[i] = &proto.ColFloat64{}
				cb.data[i].(*proto.ColFloat64).Append(v)
			}
		case time.Time:
			if col, ok := cb.data[i].(*proto.ColDateTime); ok {
				col.Append(v)
			} else {
				// Convert column type if necessary
				cb.data[i] = &proto.ColDateTime{}
				cb.data[i].(*proto.ColDateTime).Append(v)
			}
		default:
			return fmt.Errorf("unsupported type for column %s: %T", cb.columns[i], val)
		}
	}

	cb.currentSize++
	return nil
}

// IsFull checks if the buffer is full
func (cb *ColumnBuffer) IsFull() bool {
	return cb.currentSize >= cb.batchSize
}

// IsEmpty checks if the buffer is empty
func (cb *ColumnBuffer) IsEmpty() bool {
	return cb.currentSize == 0
}

// Reset clears the buffer
func (cb *ColumnBuffer) Reset() {
	for i, col := range cb.data {
		switch col.(type) {
		case *proto.ColUInt64:
			cb.data[i] = &proto.ColUInt64{}
		case *proto.ColStr:
			cb.data[i] = &proto.ColStr{}
		case *proto.ColFloat64:
			cb.data[i] = &proto.ColFloat64{}
		case *proto.ColDateTime:
			cb.data[i] = &proto.ColDateTime{}
		}
	}
	cb.currentSize = 0
}
