package clickhouse

import (
	"net"
	"time"
	"unsafe"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"
)

// ColumnWriter 提供将不同数据类型写入 ClickHouse 列的方法
type ColumnWriter struct{}

// NewColumnWriter 创建一个新的 ColumnWriter 实例
func NewColumnWriter() *ColumnWriter {
	return &ColumnWriter{}
}

// WriteStringColumn 将字符串数据写入 ClickHouse String 列
func (cw *ColumnWriter) WriteStringColumn(data []string) *proto.ColStr {
	col := &proto.ColStr{Buf: make([]byte, 0, 10), Pos: make([]proto.Position, 0)}
	for _, s := range data {
		col.Append(s)
	}
	return col
}

// WriteStringColumnFromBytes 将字节数据写入 ClickHouse String 列
func (cw *ColumnWriter) WriteStringColumnFromBytes(data []byte) *proto.ColStr {
	col := &proto.ColStr{}
	col.AppendBytes(data)
	return col
}

// WriteUInt8Column 将 uint8 数据写入 ClickHouse UInt8 列
func (cw *ColumnWriter) WriteUInt8Column(data []uint8) *proto.ColUInt8 {
	col := &proto.ColUInt8{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteUInt8ColumnFromBytes 将字节数据写入 ClickHouse UInt8 列
func (cw *ColumnWriter) WriteUInt8ColumnFromBytes(data []byte) *proto.ColUInt8 {
	col := &proto.ColUInt8{}
	// 使用不安全指针将字节切片转换为 uint8 切片
	uint8Slice := (*[]uint8)(unsafe.Pointer(&data))
	for _, v := range *uint8Slice {
		col.Append(v)
	}
	return col
}

// WriteUInt16Column 将 uint16 数据写入 ClickHouse UInt16 列
func (cw *ColumnWriter) WriteUInt16Column(data []uint16) *proto.ColUInt16 {
	col := &proto.ColUInt16{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteUInt16ColumnFromBytes 将字节数据写入 ClickHouse UInt16 列
func (cw *ColumnWriter) WriteUInt16ColumnFromBytes(data []byte) *proto.ColUInt16 {
	col := &proto.ColUInt16{}
	// 确保字节切片长度是 2 的倍数
	if len(data)%2 != 0 {
		// 如果需要，用零字节填充
		data = append(data, 0)
	}

	// 使用不安全指针将字节切片转换为 uint16 切片
	uint16Slice := (*[]uint16)(unsafe.Pointer(&data))
	*uint16Slice = (*uint16Slice)[:len(data)/2]

	for _, v := range *uint16Slice {
		col.Append(v)
	}
	return col
}

// WriteUInt32Column 将 uint32 数据写入 ClickHouse UInt32 列
func (cw *ColumnWriter) WriteUInt32Column(data []uint32) *proto.ColUInt32 {
	col := &proto.ColUInt32{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteUInt32ColumnFromBytes 将字节数据写入 ClickHouse UInt32 列
func (cw *ColumnWriter) WriteUInt32ColumnFromBytes(data []byte) *proto.ColUInt32 {
	col := &proto.ColUInt32{}
	// 确保字节切片长度是 4 的倍数
	for len(data)%4 != 0 {
		// 如果需要，用零字节填充
		data = append(data, 0)
	}

	// 使用不安全指针将字节切片转换为 uint32 切片
	uint32Slice := (*[]uint32)(unsafe.Pointer(&data))
	*uint32Slice = (*uint32Slice)[:len(data)/4]

	for _, v := range *uint32Slice {
		col.Append(v)
	}
	return col
}

// WriteUInt64Column 将 uint64 数据写入 ClickHouse UInt64 列
func (cw *ColumnWriter) WriteUInt64Column(data []uint64) *proto.ColUInt64 {
	col := &proto.ColUInt64{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteUInt64ColumnFromBytes 将字节数据写入 ClickHouse UInt64 列
func (cw *ColumnWriter) WriteUInt64ColumnFromBytes(data []byte) *proto.ColUInt64 {
	col := &proto.ColUInt64{}
	// 确保字节切片长度是 8 的倍数
	for len(data)%8 != 0 {
		// 如果需要，用零字节填充
		data = append(data, 0)
	}

	// 使用不安全指针将字节切片转换为 uint64 切片
	uint64Slice := (*[]uint64)(unsafe.Pointer(&data))
	*uint64Slice = (*uint64Slice)[:len(data)/8]

	for _, v := range *uint64Slice {
		col.Append(v)
	}
	return col
}

// WriteInt8Column 将 int8 数据写入 ClickHouse Int8 列
func (cw *ColumnWriter) WriteInt8Column(data []int8) *proto.ColInt8 {
	col := &proto.ColInt8{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteInt8ColumnFromBytes 将字节数据写入 ClickHouse Int8 列
func (cw *ColumnWriter) WriteInt8ColumnFromBytes(data []byte) *proto.ColInt8 {
	col := &proto.ColInt8{}
	// 使用不安全指针将字节切片转换为 int8 切片
	int8Slice := (*[]int8)(unsafe.Pointer(&data))
	for _, v := range *int8Slice {
		col.Append(v)
	}
	return col
}

// WriteInt16Column 将 int16 数据写入 ClickHouse Int16 列
func (cw *ColumnWriter) WriteInt16Column(data []int16) *proto.ColInt16 {
	col := &proto.ColInt16{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteInt16ColumnFromBytes 将字节数据写入 ClickHouse Int16 列
func (cw *ColumnWriter) WriteInt16ColumnFromBytes(data []byte) *proto.ColInt16 {
	col := &proto.ColInt16{}
	// 确保字节切片长度是 2 的倍数
	if len(data)%2 != 0 {
		// 如果需要，用零字节填充
		data = append(data, 0)
	}

	// 使用不安全指针将字节切片转换为 int16 切片
	int16Slice := (*[]int16)(unsafe.Pointer(&data))
	*int16Slice = (*int16Slice)[:len(data)/2]

	for _, v := range *int16Slice {
		col.Append(v)
	}
	return col
}

// WriteInt32Column 将 int32 数据写入 ClickHouse Int32 列
func (cw *ColumnWriter) WriteInt32Column(data []int32) *proto.ColInt32 {
	col := &proto.ColInt32{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteInt32ColumnFromBytes 将字节数据写入 ClickHouse Int32 列
func (cw *ColumnWriter) WriteInt32ColumnFromBytes(data []byte) *proto.ColInt32 {
	col := &proto.ColInt32{}
	// 确保字节切片长度是 4 的倍数
	for len(data)%4 != 0 {
		// 如果需要，用零字节填充
		data = append(data, 0)
	}

	// 使用不安全指针将字节切片转换为 int32 切片
	int32Slice := (*[]int32)(unsafe.Pointer(&data))
	*int32Slice = (*int32Slice)[:len(data)/4]

	for _, v := range *int32Slice {
		col.Append(v)
	}
	return col
}

// WriteInt64Column 将 int64 数据写入 ClickHouse Int64 列
func (cw *ColumnWriter) WriteInt64Column(data []int64) *proto.ColInt64 {
	col := &proto.ColInt64{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteInt64ColumnFromBytes 将字节数据写入 ClickHouse Int64 列
func (cw *ColumnWriter) WriteInt64ColumnFromBytes(data []byte) *proto.ColInt64 {
	col := &proto.ColInt64{}
	// 确保字节切片长度是 8 的倍数
	for len(data)%8 != 0 {
		// 如果需要，用零字节填充
		data = append(data, 0)
	}

	// 使用不安全指针将字节切片转换为 int64 切片
	int64Slice := (*[]int64)(unsafe.Pointer(&data))
	*int64Slice = (*int64Slice)[:len(data)/8]

	for _, v := range *int64Slice {
		col.Append(v)
	}
	return col
}

// WriteFloat32Column 将 float32 数据写入 ClickHouse Float32 列
func (cw *ColumnWriter) WriteFloat32Column(data []float32) *proto.ColFloat32 {
	col := &proto.ColFloat32{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteFloat32ColumnFromBytes 将字节数据写入 ClickHouse Float32 列
func (cw *ColumnWriter) WriteFloat32ColumnFromBytes(data []byte) *proto.ColFloat32 {
	col := &proto.ColFloat32{}
	// 确保字节切片长度是 4 的倍数
	for len(data)%4 != 0 {
		// 如果需要，用零字节填充
		data = append(data, 0)
	}

	// 使用不安全指针将字节切片转换为 float32 切片
	float32Slice := (*[]float32)(unsafe.Pointer(&data))
	*float32Slice = (*float32Slice)[:len(data)/4]

	for _, v := range *float32Slice {
		col.Append(v)
	}
	return col
}

// WriteFloat64Column 将 float64 数据写入 ClickHouse Float64 列
func (cw *ColumnWriter) WriteFloat64Column(data []float64) *proto.ColFloat64 {
	col := &proto.ColFloat64{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteFloat64ColumnFromBytes 将字节数据写入 ClickHouse Float64 列
func (cw *ColumnWriter) WriteFloat64ColumnFromBytes(data []byte) *proto.ColFloat64 {
	col := &proto.ColFloat64{}
	// 确保字节切片长度是 8 的倍数
	for len(data)%8 != 0 {
		// 如果需要，用零字节填充
		data = append(data, 0)
	}

	// 使用不安全指针将字节切片转换为 float64 切片
	float64Slice := (*[]float64)(unsafe.Pointer(&data))
	*float64Slice = (*float64Slice)[:len(data)/8]

	for _, v := range *float64Slice {
		col.Append(v)
	}
	return col
}

// WriteDateColumn 将 time.Time 数据写入 ClickHouse Date 列
func (cw *ColumnWriter) WriteDateColumn(data []time.Time) *proto.ColDate {
	col := &proto.ColDate{}
	for _, t := range data {
		col.Append(t)
	}
	return col
}

// WriteDateTimeColumn 将 time.Time 数据写入 ClickHouse DateTime 列
func (cw *ColumnWriter) WriteDateTimeColumn(data []time.Time) *proto.ColDateTime {
	col := &proto.ColDateTime{}
	for _, t := range data {
		col.Append(t)
	}
	return col
}

// WriteBoolColumn 将布尔数据写入 ClickHouse Bool 列
func (cw *ColumnWriter) WriteBoolColumn(data []bool) *proto.ColBool {
	col := &proto.ColBool{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteUUIDColumn 将 UUID 数据写入 ClickHouse UUID 列
func (cw *ColumnWriter) WriteUUIDColumn(data []uuid.UUID) *proto.ColUUID {
	col := &proto.ColUUID{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteIPv4Column 将 IPv4 数据写入 ClickHouse IPv4 列
func (cw *ColumnWriter) WriteIPv4Column(data []net.IP) *proto.ColIPv4 {
	col := &proto.ColIPv4{}
	for _, ip := range data {
		if len(ip) >= 4 {
			// 将 net.IP 转换为 IPv4 格式
			var v4 [4]byte
			copy(v4[:], ip.To4())
			col.Append(proto.IPv4(v4[0])<<24 | proto.IPv4(v4[1])<<16 | proto.IPv4(v4[2])<<8 | proto.IPv4(v4[3]))
		}
	}
	return col
}

// WriteIPv6Column 将 IPv6 数据写入 ClickHouse IPv6 列
func (cw *ColumnWriter) WriteIPv6Column(data []net.IP) *proto.ColIPv6 {
	col := &proto.ColIPv6{}
	for _, ip := range data {
		if len(ip) >= 16 {
			// 将 net.IP 转换为 IPv6 格式
			var v6 [16]byte
			copy(v6[:], ip.To16())
			col.Append(v6)
		}
	}
	return col
}

// WriteFixedStringColumn 将固定长度字符串数据写入 ClickHouse FixedString 列
func (cw *ColumnWriter) WriteFixedStringColumn(data []string, fixedSize int) *proto.ColFixedStr {
	col := &proto.ColFixedStr{}
	for _, s := range data {
		// 对于固定字符串，我们需要确保字符串是固定长度的
		if len(s) > fixedSize {
			s = s[:fixedSize]
		} else if len(s) < fixedSize {
			// 用零填充
			s += string(make([]byte, fixedSize-len(s)))
		}
		col.Append([]byte(s))
	}
	return col
}

// WriteNullableStringColumn 将可空字符串数据写入 ClickHouse 列
func (cw *ColumnWriter) WriteNullableStringColumn(data []*string) *proto.ColNullable[string] {
	strCol := &proto.ColStr{}
	col := proto.NewColNullable[string](strCol)
	for _, s := range data {
		if s != nil {
			col.Append(proto.Nullable[string]{Value: *s, Set: true})
		} else {
			col.Append(proto.Null[string]())
		}
	}
	return col
}

// WriteArrayStringColumn 将字符串数组写入 ClickHouse Array(String) 列
func (cw *ColumnWriter) WriteArrayStringColumn(data [][]string) *proto.ColArr[string] {
	col := &proto.ColArr[string]{}
	for _, arr := range data {
		col.Append(arr)
	}
	return col
}

// WriteTupleColumn 将元组数据写入 ClickHouse Tuple 列
// 这是一个更复杂的示例，展示如何将多个列组合成元组
func (cw *ColumnWriter) WriteTupleColumn(stringData []string, intData []uint64) proto.ColTuple {
	// 注意：这是简化的 - 在实践中，元组列需要更复杂的设置
	tupleCol := make(proto.ColTuple, 2)

	strCol := &proto.ColStr{}
	for _, s := range stringData {
		strCol.Append(s)
	}

	intCol := &proto.ColUInt64{}
	for _, v := range intData {
		intCol.Append(v)
	}

	tupleCol[0] = strCol
	tupleCol[1] = intCol

	return tupleCol
}
