package clickhouse

import (
	"net"
	"time"
	"unsafe"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"
)

// ColumnWriter provides methods for writing different data types to ClickHouse columns
type ColumnWriter struct{}

// NewColumnWriter creates a new ColumnWriter instance
func NewColumnWriter() *ColumnWriter {
	return &ColumnWriter{}
}

// WriteStringColumn writes string data to a ClickHouse String column
func (cw *ColumnWriter) WriteStringColumn(data []string) *proto.ColStr {
	col := &proto.ColStr{Buf: make([]byte, 0, 10), Pos: make([]proto.Position, 0)}
	for _, s := range data {
		col.Append(s)
	}
	return col
}

// WriteStringColumnFromBytes writes byte data to a ClickHouse String column
func (cw *ColumnWriter) WriteStringColumnFromBytes(data []byte) *proto.ColStr {
	col := &proto.ColStr{}
	col.AppendBytes(data)
	return col
}

// WriteUInt8Column writes uint8 data to a ClickHouse UInt8 column
func (cw *ColumnWriter) WriteUInt8Column(data []uint8) *proto.ColUInt8 {
	col := &proto.ColUInt8{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteUInt8ColumnFromBytes writes uint8 data from byte slice to a ClickHouse UInt8 column
func (cw *ColumnWriter) WriteUInt8ColumnFromBytes(data []byte) *proto.ColUInt8 {
	col := &proto.ColUInt8{}
	// Convert byte slice to uint8 slice using unsafe pointer
	uint8Slice := (*[]uint8)(unsafe.Pointer(&data))
	for _, v := range *uint8Slice {
		col.Append(v)
	}
	return col
}

// WriteUInt16Column writes uint16 data to a ClickHouse UInt16 column
func (cw *ColumnWriter) WriteUInt16Column(data []uint16) *proto.ColUInt16 {
	col := &proto.ColUInt16{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteUInt16ColumnFromBytes writes uint16 data from byte slice to a ClickHouse UInt16 column
func (cw *ColumnWriter) WriteUInt16ColumnFromBytes(data []byte) *proto.ColUInt16 {
	col := &proto.ColUInt16{}
	// Ensure the byte slice length is a multiple of 2
	if len(data)%2 != 0 {
		// Pad with zero byte if needed
		data = append(data, 0)
	}

	// Convert byte slice to uint16 slice using unsafe pointer
	uint16Slice := (*[]uint16)(unsafe.Pointer(&data))
	*uint16Slice = (*uint16Slice)[:len(data)/2]

	for _, v := range *uint16Slice {
		col.Append(v)
	}
	return col
}

// WriteUInt32Column writes uint32 data to a ClickHouse UInt32 column
func (cw *ColumnWriter) WriteUInt32Column(data []uint32) *proto.ColUInt32 {
	col := &proto.ColUInt32{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteUInt32ColumnFromBytes writes uint32 data from byte slice to a ClickHouse UInt32 column
func (cw *ColumnWriter) WriteUInt32ColumnFromBytes(data []byte) *proto.ColUInt32 {
	col := &proto.ColUInt32{}
	// Ensure the byte slice length is a multiple of 4
	for len(data)%4 != 0 {
		// Pad with zero bytes if needed
		data = append(data, 0)
	}

	// Convert byte slice to uint32 slice using unsafe pointer
	uint32Slice := (*[]uint32)(unsafe.Pointer(&data))
	*uint32Slice = (*uint32Slice)[:len(data)/4]

	for _, v := range *uint32Slice {
		col.Append(v)
	}
	return col
}

// WriteUInt64Column writes uint64 data to a ClickHouse UInt64 column
func (cw *ColumnWriter) WriteUInt64Column(data []uint64) *proto.ColUInt64 {
	col := &proto.ColUInt64{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteUInt64ColumnFromBytes writes uint64 data from byte slice to a ClickHouse UInt64 column
func (cw *ColumnWriter) WriteUInt64ColumnFromBytes(data []byte) *proto.ColUInt64 {
	col := &proto.ColUInt64{}
	// Ensure the byte slice length is a multiple of 8
	for len(data)%8 != 0 {
		// Pad with zero bytes if needed
		data = append(data, 0)
	}

	// Convert byte slice to uint64 slice using unsafe pointer
	uint64Slice := (*[]uint64)(unsafe.Pointer(&data))
	*uint64Slice = (*uint64Slice)[:len(data)/8]

	for _, v := range *uint64Slice {
		col.Append(v)
	}
	return col
}

// WriteInt8Column writes int8 data to a ClickHouse Int8 column
func (cw *ColumnWriter) WriteInt8Column(data []int8) *proto.ColInt8 {
	col := &proto.ColInt8{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteInt8ColumnFromBytes writes int8 data from byte slice to a ClickHouse Int8 column
func (cw *ColumnWriter) WriteInt8ColumnFromBytes(data []byte) *proto.ColInt8 {
	col := &proto.ColInt8{}
	// Convert byte slice to int8 slice using unsafe pointer
	int8Slice := (*[]int8)(unsafe.Pointer(&data))
	for _, v := range *int8Slice {
		col.Append(v)
	}
	return col
}

// WriteInt16Column writes int16 data to a ClickHouse Int16 column
func (cw *ColumnWriter) WriteInt16Column(data []int16) *proto.ColInt16 {
	col := &proto.ColInt16{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteInt16ColumnFromBytes writes int16 data from byte slice to a ClickHouse Int16 column
func (cw *ColumnWriter) WriteInt16ColumnFromBytes(data []byte) *proto.ColInt16 {
	col := &proto.ColInt16{}
	// Ensure the byte slice length is a multiple of 2
	if len(data)%2 != 0 {
		// Pad with zero byte if needed
		data = append(data, 0)
	}

	// Convert byte slice to int16 slice using unsafe pointer
	int16Slice := (*[]int16)(unsafe.Pointer(&data))
	*int16Slice = (*int16Slice)[:len(data)/2]

	for _, v := range *int16Slice {
		col.Append(v)
	}
	return col
}

// WriteInt32Column writes int32 data to a ClickHouse Int32 column
func (cw *ColumnWriter) WriteInt32Column(data []int32) *proto.ColInt32 {
	col := &proto.ColInt32{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteInt32ColumnFromBytes writes int32 data from byte slice to a ClickHouse Int32 column
func (cw *ColumnWriter) WriteInt32ColumnFromBytes(data []byte) *proto.ColInt32 {
	col := &proto.ColInt32{}
	// Ensure the byte slice length is a multiple of 4
	for len(data)%4 != 0 {
		// Pad with zero bytes if needed
		data = append(data, 0)
	}

	// Convert byte slice to int32 slice using unsafe pointer
	int32Slice := (*[]int32)(unsafe.Pointer(&data))
	*int32Slice = (*int32Slice)[:len(data)/4]

	for _, v := range *int32Slice {
		col.Append(v)
	}
	return col
}

// WriteInt64Column writes int64 data to a ClickHouse Int64 column
func (cw *ColumnWriter) WriteInt64Column(data []int64) *proto.ColInt64 {
	col := &proto.ColInt64{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteInt64ColumnFromBytes writes int64 data from byte slice to a ClickHouse Int64 column
func (cw *ColumnWriter) WriteInt64ColumnFromBytes(data []byte) *proto.ColInt64 {
	col := &proto.ColInt64{}
	// Ensure the byte slice length is a multiple of 8
	for len(data)%8 != 0 {
		// Pad with zero bytes if needed
		data = append(data, 0)
	}

	// Convert byte slice to int64 slice using unsafe pointer
	int64Slice := (*[]int64)(unsafe.Pointer(&data))
	*int64Slice = (*int64Slice)[:len(data)/8]

	for _, v := range *int64Slice {
		col.Append(v)
	}
	return col
}

// WriteFloat32Column writes float32 data to a ClickHouse Float32 column
func (cw *ColumnWriter) WriteFloat32Column(data []float32) *proto.ColFloat32 {
	col := &proto.ColFloat32{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteFloat32ColumnFromBytes writes float32 data from byte slice to a ClickHouse Float32 column
func (cw *ColumnWriter) WriteFloat32ColumnFromBytes(data []byte) *proto.ColFloat32 {
	col := &proto.ColFloat32{}
	// Ensure the byte slice length is a multiple of 4
	for len(data)%4 != 0 {
		// Pad with zero bytes if needed
		data = append(data, 0)
	}

	// Convert byte slice to float32 slice using unsafe pointer
	float32Slice := (*[]float32)(unsafe.Pointer(&data))
	*float32Slice = (*float32Slice)[:len(data)/4]

	for _, v := range *float32Slice {
		col.Append(v)
	}
	return col
}

// WriteFloat64Column writes float64 data to a ClickHouse Float64 column
func (cw *ColumnWriter) WriteFloat64Column(data []float64) *proto.ColFloat64 {
	col := &proto.ColFloat64{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteFloat64ColumnFromBytes writes float64 data from byte slice to a ClickHouse Float64 column
func (cw *ColumnWriter) WriteFloat64ColumnFromBytes(data []byte) *proto.ColFloat64 {
	col := &proto.ColFloat64{}
	// Ensure the byte slice length is a multiple of 8
	for len(data)%8 != 0 {
		// Pad with zero bytes if needed
		data = append(data, 0)
	}

	// Convert byte slice to float64 slice using unsafe pointer
	float64Slice := (*[]float64)(unsafe.Pointer(&data))
	*float64Slice = (*float64Slice)[:len(data)/8]

	for _, v := range *float64Slice {
		col.Append(v)
	}
	return col
}

// WriteDateColumn writes time.Time data to a ClickHouse Date column
func (cw *ColumnWriter) WriteDateColumn(data []time.Time) *proto.ColDate {
	col := &proto.ColDate{}
	for _, t := range data {
		col.Append(t)
	}
	return col
}

// WriteDateTimeColumn writes time.Time data to a ClickHouse DateTime column
func (cw *ColumnWriter) WriteDateTimeColumn(data []time.Time) *proto.ColDateTime {
	col := &proto.ColDateTime{}
	for _, t := range data {
		col.Append(t)
	}
	return col
}

// WriteBoolColumn writes boolean data to a ClickHouse Bool column
func (cw *ColumnWriter) WriteBoolColumn(data []bool) *proto.ColBool {
	col := &proto.ColBool{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteUUIDColumn writes UUID data to a ClickHouse UUID column
func (cw *ColumnWriter) WriteUUIDColumn(data []uuid.UUID) *proto.ColUUID {
	col := &proto.ColUUID{}
	for _, v := range data {
		col.Append(v)
	}
	return col
}

// WriteIPv4Column writes IPv4 data to a ClickHouse IPv4 column
func (cw *ColumnWriter) WriteIPv4Column(data []net.IP) *proto.ColIPv4 {
	col := &proto.ColIPv4{}
	for _, ip := range data {
		if len(ip) >= 4 {
			// Convert net.IP to IPv4 format
			var v4 [4]byte
			copy(v4[:], ip.To4())
			col.Append(proto.IPv4(v4[0])<<24 | proto.IPv4(v4[1])<<16 | proto.IPv4(v4[2])<<8 | proto.IPv4(v4[3]))
		}
	}
	return col
}

// WriteIPv6Column writes IPv6 data to a ClickHouse IPv6 column
func (cw *ColumnWriter) WriteIPv6Column(data []net.IP) *proto.ColIPv6 {
	col := &proto.ColIPv6{}
	for _, ip := range data {
		if len(ip) >= 16 {
			// Convert net.IP to IPv6 format
			var v6 [16]byte
			copy(v6[:], ip.To16())
			col.Append(v6)
		}
	}
	return col
}

// WriteFixedStringColumn writes fixed-length string data to a ClickHouse FixedString column
func (cw *ColumnWriter) WriteFixedStringColumn(data []string, fixedSize int) *proto.ColFixedStr {
	col := &proto.ColFixedStr{}
	for _, s := range data {
		// For fixed string, we need to ensure the string is of fixed length
		if len(s) > fixedSize {
			s = s[:fixedSize]
		} else if len(s) < fixedSize {
			// Pad with zeros
			s += string(make([]byte, fixedSize-len(s)))
		}
		col.Append([]byte(s))
	}
	return col
}

// WriteNullableStringColumn writes nullable string data to a ClickHouse column
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

// WriteArrayStringColumn writes array of strings to a ClickHouse Array(String) column
func (cw *ColumnWriter) WriteArrayStringColumn(data [][]string) *proto.ColArr[string] {
	col := &proto.ColArr[string]{}
	for _, arr := range data {
		col.Append(arr)
	}
	return col
}

// WriteTupleColumn writes tuple data to a ClickHouse Tuple column
// This is a more complex example showing how to combine multiple columns into a tuple
func (cw *ColumnWriter) WriteTupleColumn(stringData []string, intData []uint64) proto.ColTuple {
	// Note: This is simplified - in practice, tuple columns require more complex setup
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
