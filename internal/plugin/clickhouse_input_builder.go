// clickhouse_input_builder.go 构建ClickHouse的proto.Input
package plugin

import (
	"berry-rapids-go/internal/model"
	"berry-rapids-go/pkg/clickhouse"
	"fmt"
	"math"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"
)

// InputBuilder 构建ClickHouse Input
type InputBuilder struct {
	columnWriter *clickhouse.ColumnWriter
	mapping      *MappingConfig
}

// NewInputBuilder 创建新的InputBuilder
func NewInputBuilder(mapping *MappingConfig) *InputBuilder {
	return &InputBuilder{
		columnWriter: clickhouse.NewColumnWriter(),
		mapping:      mapping,
	}
}

// BuildInput 从字段数据构建ClickHouse Input
func (ib *InputBuilder) BuildInput(fieldData map[string][]byte) (*proto.Input, error) {
	if fieldData == nil {
		return nil, fmt.Errorf("field data cannot be nil")
	}

	// 创建Input
	input := make(proto.Input, len(ib.mapping.Fields))

	// 为每个字段创建列
	for i, field := range ib.mapping.Fields {
		msgpackData, exists := fieldData[field.MsgPackFieldName]
		if !exists {
			return nil, fmt.Errorf("field %s not found in data", field.MsgPackFieldName)
		}

		colInput, err := ib.buildColumnInput(msgpackData, field)
		if err != nil {
			return nil, fmt.Errorf("failed to build column input for field %s: %w", field.MsgPackFieldName, err)
		}

		input[i] = proto.InputColumn{
			Name: field.CHColumnName,
			Data: colInput,
		}
	}

	return &input, nil
}

// BuildInputFromFieldBatch 从 FieldBatch 构建ClickHouse Input（单个记录）
func (ib *InputBuilder) BuildInputFromFieldBatch(fieldBatch *model.FieldBatch) (*proto.Input, error) {
	if fieldBatch == nil {
		return nil, fmt.Errorf("field batch cannot be nil")
	}

	// 获取底层数据
	data := fieldBatch.GetData()
	if data == nil {
		return nil, fmt.Errorf("field batch data cannot be nil")
	}

	// 提取每行的第一个元素（单个记录）
	fieldData := make(map[string][]byte)
	for fieldName, values := range data {
		if len(values) > 0 && values[0] != nil {
			fieldData[fieldName] = values[0]
		} else {
			fieldData[fieldName] = nil
		}
	}

	return ib.BuildInput(fieldData)
}

// BuildInputBatch 从批量字段数据构建ClickHouse Input
// fieldDataBatch: FieldBatch 结构体，包含字段数据和统计信息
func (ib *InputBuilder) BuildInputBatch(fieldDataBatch *model.FieldBatch) (*proto.Input, error) {
	if fieldDataBatch == nil {
		return nil, fmt.Errorf("field data batch cannot be nil")
	}

	// 获取底层数据
	data := fieldDataBatch.GetData()
	if data == nil {
		return nil, fmt.Errorf("field data batch cannot be empty")
	}

	// 获取记录数
	rowCount := fieldDataBatch.GetRowCount()
	if rowCount == 0 {
		return nil, fmt.Errorf("no records in batch")
	}

	// 创建Input
	input := make(proto.Input, len(ib.mapping.Fields))

	// 为每个字段创建列
	for i, field := range ib.mapping.Fields {
		fieldDataSlice, exists := data[field.MsgPackFieldName]
		if !exists {
			return nil, fmt.Errorf("field %s not found in batch data", field.MsgPackFieldName)
		}

		colInput, err := ib.buildColumnInputBatch(fieldDataSlice, field)
		if err != nil {
			return nil, fmt.Errorf("failed to build column input for field %s: %w", field.MsgPackFieldName, err)
		}

		input[i] = proto.InputColumn{
			Name: field.CHColumnName,
			Data: colInput,
		}
	}

	return &input, nil
}

// buildColumnInputBatch 根据批量字段数据构建列输入
func (ib *InputBuilder) buildColumnInputBatch(dataSlice [][]byte, field FieldMapping) (proto.Column, error) {
	if dataSlice == nil || len(dataSlice) == 0 {
		// 处理空值
		return ib.buildNullColumn(field.CHColumnType)
	}

	switch field.CHColumnType {
	case FieldTypeString:
		return ib.buildStringColumnBatch(dataSlice), nil

	case FieldTypeInt8:
		return ib.buildInt8ColumnBatch(dataSlice), nil

	case FieldTypeInt16:
		return ib.buildInt16ColumnBatch(dataSlice), nil

	case FieldTypeInt32:
		return ib.buildInt32ColumnBatch(dataSlice), nil

	case FieldTypeInt64:
		return ib.buildInt64ColumnBatch(dataSlice), nil

	case FieldTypeUInt8:
		return ib.buildUInt8ColumnBatch(dataSlice), nil

	case FieldTypeUInt16:
		return ib.buildUInt16ColumnBatch(dataSlice), nil

	case FieldTypeUInt32:
		return ib.buildUInt32ColumnBatch(dataSlice), nil

	case FieldTypeUInt64:
		return ib.buildUInt64ColumnBatch(dataSlice), nil

	case FieldTypeFloat32:
		return ib.buildFloat32ColumnBatch(dataSlice), nil

	case FieldTypeFloat64:
		return ib.buildFloat64ColumnBatch(dataSlice), nil

	case FieldTypeBool:
		return ib.buildBoolColumnBatch(dataSlice), nil

	case FieldTypeDateTime:
		return ib.buildDateTimeColumnBatch(dataSlice), nil

	case FieldTypeDate:
		return ib.buildDateColumnBatch(dataSlice), nil

	case FieldTypeUUID:
		return ib.buildUUIDColumnBatch(dataSlice), nil

	case FieldTypeIPv4:
		return ib.buildIPv4ColumnBatch(dataSlice), nil

	case FieldTypeIPv6:
		return ib.buildIPv6ColumnBatch(dataSlice), nil

	default:
		return nil, fmt.Errorf("unsupported ClickHouse column type: %s", field.CHColumnType)
	}
}

// buildStringColumnBatch 构建字符串列（批量，使用AppendBytes实现零拷贝）
func (ib *InputBuilder) buildStringColumnBatch(dataSlice [][]byte) proto.Column {
	col := &proto.ColStr{Buf: make([]byte, 0), Pos: make([]proto.Position, 0, len(dataSlice))}
	for _, data := range dataSlice {
		if data != nil {
			col.AppendBytes(data) // 使用AppendBytes避免创建临时string对象
		} else {
			col.AppendBytes([]byte("")) // 空字符串
		}
	}
	return col
}

// buildInt8ColumnBatch 构建int8列（批量）
func (ib *InputBuilder) buildInt8ColumnBatch(dataSlice [][]byte) proto.Column {
	col := &proto.ColInt8{}
	for _, data := range dataSlice {
		if data != nil && len(data) >= 1 {
			col.Append(int8(data[0]))
		} else {
			col.Append(0)
		}
	}
	return col
}

// buildInt16ColumnBatch 构建int16列（批量）
func (ib *InputBuilder) buildInt16ColumnBatch(dataSlice [][]byte) proto.Column {
	col := &proto.ColInt16{}
	for _, data := range dataSlice {
		if data != nil && len(data) >= 2 {
			val := int16(data[0])<<8 | int16(data[1])
			col.Append(val)
		} else {
			col.Append(0)
		}
	}
	return col
}

// buildInt32ColumnBatch 构建int32列（批量）
func (ib *InputBuilder) buildInt32ColumnBatch(dataSlice [][]byte) proto.Column {
	col := &proto.ColInt32{}
	for _, data := range dataSlice {
		if data != nil && len(data) >= 4 {
			val := int32(data[0])<<24 | int32(data[1])<<16 | int32(data[2])<<8 | int32(data[3])
			col.Append(val)
		} else {
			col.Append(0)
		}
	}
	return col
}

// buildInt64ColumnBatch 构建int64列（批量）
func (ib *InputBuilder) buildInt64ColumnBatch(dataSlice [][]byte) proto.Column {
	col := &proto.ColInt64{}
	for _, data := range dataSlice {
		if data != nil && len(data) >= 8 {
			val := int64(data[0])<<56 | int64(data[1])<<48 | int64(data[2])<<40 | int64(data[3])<<32 |
				int64(data[4])<<24 | int64(data[5])<<16 | int64(data[6])<<8 | int64(data[7])
			col.Append(val)
		} else {
			col.Append(0)
		}
	}
	return col
}

// buildUInt8ColumnBatch 构建uint8列（批量）
func (ib *InputBuilder) buildUInt8ColumnBatch(dataSlice [][]byte) proto.Column {
	col := &proto.ColUInt8{}
	for _, data := range dataSlice {
		if data != nil && len(data) >= 1 {
			col.Append(data[0])
		} else {
			col.Append(0)
		}
	}
	return col
}

// buildUInt16ColumnBatch 构建uint16列（批量）
func (ib *InputBuilder) buildUInt16ColumnBatch(dataSlice [][]byte) proto.Column {
	col := &proto.ColUInt16{}
	for _, data := range dataSlice {
		if data != nil && len(data) >= 2 {
			val := uint16(data[0])<<8 | uint16(data[1])
			col.Append(val)
		} else {
			col.Append(0)
		}
	}
	return col
}

// buildUInt32ColumnBatch 构建uint32列（批量）
func (ib *InputBuilder) buildUInt32ColumnBatch(dataSlice [][]byte) proto.Column {
	col := &proto.ColUInt32{}
	for _, data := range dataSlice {
		if data != nil && len(data) >= 4 {
			val := uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
			col.Append(val)
		} else {
			col.Append(0)
		}
	}
	return col
}

// buildUInt64ColumnBatch 构建uint64列（批量）
func (ib *InputBuilder) buildUInt64ColumnBatch(dataSlice [][]byte) proto.Column {
	col := &proto.ColUInt64{}
	for _, data := range dataSlice {
		if data != nil && len(data) >= 8 {
			val := uint64(data[0])<<56 | uint64(data[1])<<48 | uint64(data[2])<<40 | uint64(data[3])<<32 |
				uint64(data[4])<<24 | uint64(data[5])<<16 | uint64(data[6])<<8 | uint64(data[7])
			col.Append(val)
		} else {
			col.Append(0)
		}
	}
	return col
}

// buildFloat32ColumnBatch 构建float32列（批量）
func (ib *InputBuilder) buildFloat32ColumnBatch(dataSlice [][]byte) proto.Column {
	col := &proto.ColFloat32{}
	for _, data := range dataSlice {
		if data != nil && len(data) >= 4 {
			bits := uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
			val := math.Float32frombits(bits)
			col.Append(val)
		} else {
			col.Append(0)
		}
	}
	return col
}

// buildFloat64ColumnBatch 构建float64列（批量）
func (ib *InputBuilder) buildFloat64ColumnBatch(dataSlice [][]byte) proto.Column {
	col := &proto.ColFloat64{}
	for _, data := range dataSlice {
		if data != nil && len(data) >= 8 {
			bits := uint64(data[0])<<56 | uint64(data[1])<<48 | uint64(data[2])<<40 | uint64(data[3])<<32 |
				uint64(data[4])<<24 | uint64(data[5])<<16 | uint64(data[6])<<8 | uint64(data[7])
			val := math.Float64frombits(bits)
			col.Append(val)
		} else {
			col.Append(0)
		}
	}
	return col
}

// buildBoolColumnBatch 构建布尔列（批量）
func (ib *InputBuilder) buildBoolColumnBatch(dataSlice [][]byte) proto.Column {
	col := &proto.ColBool{}
	for _, data := range dataSlice {
		if data != nil && len(data) >= 1 {
			col.Append(data[0] != 0)
		} else {
			col.Append(false)
		}
	}
	return col
}

// buildDateTimeColumnBatch 构建日期时间列（批量）
func (ib *InputBuilder) buildDateTimeColumnBatch(dataSlice [][]byte) proto.Column {
	col := &proto.ColDateTime{}
	for _, data := range dataSlice {
		if data != nil && len(data) >= 8 {
			timestamp := int64(data[0])<<56 | int64(data[1])<<48 | int64(data[2])<<40 | int64(data[3])<<32 |
				int64(data[4])<<24 | int64(data[5])<<16 | int64(data[6])<<8 | int64(data[7])
			col.Append(time.Unix(timestamp, 0))
		} else {
			col.Append(time.Time{})
		}
	}
	return col
}

// buildDateColumnBatch 构建日期列（批量）
func (ib *InputBuilder) buildDateColumnBatch(dataSlice [][]byte) proto.Column {
	col := &proto.ColDate{}
	for _, data := range dataSlice {
		if data != nil && len(data) >= 8 {
			timestamp := int64(data[0])<<56 | int64(data[1])<<48 | int64(data[2])<<40 | int64(data[3])<<32 |
				int64(data[4])<<24 | int64(data[5])<<16 | int64(data[6])<<8 | int64(data[7])
			col.Append(time.Unix(timestamp, 0))
		} else {
			col.Append(time.Time{})
		}
	}
	return col
}

// buildUUIDColumnBatch 构建UUID列（批量）
func (ib *InputBuilder) buildUUIDColumnBatch(dataSlice [][]byte) proto.Column {
	col := &proto.ColUUID{}
	for _, data := range dataSlice {
		if data != nil && len(data) >= 16 {
			var u uuid.UUID
			copy(u[:], data[:16])
			col.Append(u)
		} else {
			col.Append(uuid.UUID{})
		}
	}
	return col
}

// buildIPv4ColumnBatch 构建IPv4列（批量）
func (ib *InputBuilder) buildIPv4ColumnBatch(dataSlice [][]byte) proto.Column {
	col := &proto.ColIPv4{}
	for _, data := range dataSlice {
		if data != nil && len(data) >= 4 {
			var ip [4]byte
			copy(ip[:], data[:4])
			col.Append(proto.IPv4(ip[0])<<24 | proto.IPv4(ip[1])<<16 | proto.IPv4(ip[2])<<8 | proto.IPv4(ip[3]))
		} else {
			col.Append(0)
		}
	}
	return col
}

// buildIPv6ColumnBatch 构建IPv6列（批量）
func (ib *InputBuilder) buildIPv6ColumnBatch(dataSlice [][]byte) proto.Column {
	col := &proto.ColIPv6{}
	for _, data := range dataSlice {
		if data != nil && len(data) >= 16 {
			var ip [16]byte
			copy(ip[:], data[:16])
			col.Append(ip)
		} else {
			col.Append([16]byte{})
		}
	}
	return col
}

// buildColumnInput 根据字段类型构建列输入
func (ib *InputBuilder) buildColumnInput(data []byte, field FieldMapping) (proto.Column, error) {
	if data == nil {
		// 处理空值
		return ib.buildNullColumn(field.CHColumnType)
	}

	switch field.CHColumnType {
	case FieldTypeString:
		return ib.columnWriter.WriteStringColumnFromBytes(data), nil

	case FieldTypeInt8:
		return ib.columnWriter.WriteInt8ColumnFromBytes(data), nil

	case FieldTypeInt16:
		return ib.columnWriter.WriteInt16ColumnFromBytes(data), nil

	case FieldTypeInt32:
		return ib.columnWriter.WriteInt32ColumnFromBytes(data), nil

	case FieldTypeInt64:
		return ib.columnWriter.WriteInt64ColumnFromBytes(data), nil

	case FieldTypeUInt8:
		return ib.columnWriter.WriteUInt8ColumnFromBytes(data), nil

	case FieldTypeUInt16:
		return ib.columnWriter.WriteUInt16ColumnFromBytes(data), nil

	case FieldTypeUInt32:
		return ib.columnWriter.WriteUInt32ColumnFromBytes(data), nil

	case FieldTypeUInt64:
		return ib.columnWriter.WriteUInt64ColumnFromBytes(data), nil

	case FieldTypeFloat32:
		return ib.columnWriter.WriteFloat32ColumnFromBytes(data), nil

	case FieldTypeFloat64:
		return ib.columnWriter.WriteFloat64ColumnFromBytes(data), nil

	case FieldTypeBool:
		return ib.buildBoolColumn(data), nil

	case FieldTypeDateTime:
		return ib.buildDateTimeColumn(data), nil

	case FieldTypeDate:
		return ib.buildDateColumn(data), nil

	case FieldTypeUUID:
		return ib.buildUUIDColumn(data), nil

	case FieldTypeIPv4:
		return ib.buildIPv4Column(data), nil

	case FieldTypeIPv6:
		return ib.buildIPv6Column(data), nil

	default:
		return nil, fmt.Errorf("unsupported ClickHouse column type: %s", field.CHColumnType)
	}
}

// buildNullColumn 构建空值列
func (ib *InputBuilder) buildNullColumn(fieldType FieldType) (proto.Column, error) {
	// 根据类型创建空列
	switch fieldType {
	case FieldTypeString:
		return ib.columnWriter.WriteStringColumn([]string{""}), nil
	case FieldTypeInt8:
		return ib.columnWriter.WriteInt8Column([]int8{0}), nil
	case FieldTypeInt16:
		return ib.columnWriter.WriteInt16Column([]int16{0}), nil
	case FieldTypeInt32:
		return ib.columnWriter.WriteInt32Column([]int32{0}), nil
	case FieldTypeInt64:
		return ib.columnWriter.WriteInt64Column([]int64{0}), nil
	case FieldTypeUInt8:
		return ib.columnWriter.WriteUInt8Column([]uint8{0}), nil
	case FieldTypeUInt16:
		return ib.columnWriter.WriteUInt16Column([]uint16{0}), nil
	case FieldTypeUInt32:
		return ib.columnWriter.WriteUInt32Column([]uint32{0}), nil
	case FieldTypeUInt64:
		return ib.columnWriter.WriteUInt64Column([]uint64{0}), nil
	case FieldTypeFloat32:
		return ib.columnWriter.WriteFloat32Column([]float32{0}), nil
	case FieldTypeFloat64:
		return ib.columnWriter.WriteFloat64Column([]float64{0}), nil
	case FieldTypeBool:
		return ib.columnWriter.WriteBoolColumn([]bool{false}), nil
	case FieldTypeDateTime:
		return ib.columnWriter.WriteDateTimeColumn([]time.Time{}), nil
	case FieldTypeDate:
		return ib.columnWriter.WriteDateColumn([]time.Time{}), nil
	default:
		return nil, fmt.Errorf("unsupported field type for null column: %s", fieldType)
	}
}

// buildBoolColumn 构建布尔列
func (ib *InputBuilder) buildBoolColumn(data []byte) proto.Column {
	col := &proto.ColBool{}
	for _, b := range data {
		col.Append(b != 0)
	}
	return col
}

// buildDateTimeColumn 构建日期时间列
func (ib *InputBuilder) buildDateTimeColumn(data []byte) proto.Column {
	col := &proto.ColDateTime{}
	// 将字节数据转换为时间戳
	timestamp := int64(0)
	if len(data) >= 8 {
		timestamp = int64(data[0])<<56 | int64(data[1])<<48 | int64(data[2])<<40 | int64(data[3])<<32 |
			int64(data[4])<<24 | int64(data[5])<<16 | int64(data[6])<<8 | int64(data[7])
	}
	col.Append(time.Unix(timestamp, 0))
	return col
}

// buildDateColumn 构建日期列
func (ib *InputBuilder) buildDateColumn(data []byte) proto.Column {
	col := &proto.ColDate{}
	// 将字节数据转换为时间戳
	timestamp := int64(0)
	if len(data) >= 8 {
		timestamp = int64(data[0])<<56 | int64(data[1])<<48 | int64(data[2])<<40 | int64(data[3])<<32 |
			int64(data[4])<<24 | int64(data[5])<<16 | int64(data[6])<<8 | int64(data[7])
	}
	col.Append(time.Unix(timestamp, 0))
	return col
}

// buildUUIDColumn 构建UUID列
func (ib *InputBuilder) buildUUIDColumn(data []byte) proto.Column {
	col := &proto.ColUUID{}
	if len(data) >= 16 {
		var u uuid.UUID
		copy(u[:], data[:16])
		col.Append(u)
	}
	return col
}

// buildIPv4Column 构建IPv4列
func (ib *InputBuilder) buildIPv4Column(data []byte) proto.Column {
	col := &proto.ColIPv4{}
	if len(data) >= 4 {
		var ip [4]byte
		copy(ip[:], data[:4])
		col.Append(proto.IPv4(ip[0])<<24 | proto.IPv4(ip[1])<<16 | proto.IPv4(ip[2])<<8 | proto.IPv4(ip[3]))
	}
	return col
}

// buildIPv6Column 构建IPv6列
func (ib *InputBuilder) buildIPv6Column(data []byte) proto.Column {
	col := &proto.ColIPv6{}
	if len(data) >= 16 {
		var ip [16]byte
		copy(ip[:], data[:16])
		col.Append(ip)
	}
	return col
}

// GetTableName 获取目标表名
func (ib *InputBuilder) GetTableName() string {
	return ib.mapping.TableName
}

// GetColumnNames 获取所有列名
func (ib *InputBuilder) GetColumnNames() []string {
	return ib.mapping.GetColumnNames()
}
