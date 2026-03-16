// msgpack_field_extractor.go 从MessagePack中零拷贝提取字段数据
package plugin

import (
	"berry-rapids-go/internal/model"
	"encoding/binary"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// ByteBuffer 提供对字节数组的结构化访问（零拷贝）
type ByteBuffer struct {
	data   []byte // 底层字节数据
	offset int    // 当前读取位置
	limit  int    // 限制位置
}

// NewByteBuffer 创建一个新的 ByteBuffer
func NewByteBuffer(data []byte) *ByteBuffer {
	return &ByteBuffer{
		data:   data,
		offset: 0,
		limit:  len(data),
	}
}

// GetBytes 获取指定位置的字节数据（零拷贝）
func (bb *ByteBuffer) GetBytes(pos, length int) ([]byte, error) {
	if pos < 0 || length < 0 || pos+length > bb.limit {
		return nil, fmt.Errorf("invalid range: pos=%d, length=%d, limit=%d", pos, length, bb.limit)
	}
	return bb.data[pos : pos+length : pos+length], nil
}

// GetInt8 读取 int8 值
func (bb *ByteBuffer) GetInt8(pos int) (int8, error) {
	if pos < 0 || pos+1 > bb.limit {
		return 0, fmt.Errorf("invalid position: pos=%d, limit=%d", pos, bb.limit)
	}
	return int8(bb.data[pos]), nil
}

// GetInt16 读取 int16 值（大端序）
func (bb *ByteBuffer) GetInt16(pos int) (int16, error) {
	if pos < 0 || pos+2 > bb.limit {
		return 0, fmt.Errorf("invalid position: pos=%d, limit=%d", pos, bb.limit)
	}
	return int16(binary.BigEndian.Uint16(bb.data[pos : pos+2])), nil
}

// GetInt32 读取 int32 值（大端序）
func (bb *ByteBuffer) GetInt32(pos int) (int32, error) {
	if pos < 0 || pos+4 > bb.limit {
		return 0, fmt.Errorf("invalid position: pos=%d, limit=%d", pos, bb.limit)
	}
	return int32(binary.BigEndian.Uint32(bb.data[pos : pos+4])), nil
}

// GetInt64 读取 int64 值（大端序）
func (bb *ByteBuffer) GetInt64(pos int) (int64, error) {
	if pos < 0 || pos+8 > bb.limit {
		return 0, fmt.Errorf("invalid position: pos=%d, limit=%d", pos, bb.limit)
	}
	return int64(binary.BigEndian.Uint64(bb.data[pos : pos+8])), nil
}

// ToSliceUnsafe 使用 unsafe 将字节数组转换为指定类型的切片（零拷贝）
// 这是一个泛型函数，用于将 ByteBuffer 的数据转换为指定类型的切片
func ToSliceUnsafe[T any](bb *ByteBuffer) ([]T, error) {
	if bb == nil || len(bb.data) == 0 {
		return []T{}, nil
	}

	var t T
	typeSize := int(unsafe.Sizeof(t))

	if len(bb.data)%typeSize != 0 {
		return nil, fmt.Errorf("data length %d is not a multiple of type size %d", len(bb.data), typeSize)
	}

	sliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&bb.data))
	newSliceHeader := reflect.SliceHeader{
		Data: sliceHeader.Data,
		Len:  len(bb.data) / typeSize,
		Cap:  len(bb.data) / typeSize,
	}

	return *(*[]T)(unsafe.Pointer(&newSliceHeader)), nil
}

// FieldExtractor 从MessagePack数据中提取字段
type FieldExtractor struct {
	originalData []byte // 保存原始字节数据的引用，用于零拷贝
	pos          int    // 当前读取位置
}

// NewFieldExtractor 创建新的字段提取器
func NewFieldExtractor(data []byte) *FieldExtractor {
	return &FieldExtractor{
		originalData: data,
		pos:          0,
	}
}

// ExtractFieldData 提取指定字段的数据
// 返回字段的原始字节数据，不发生额外的内存分配（零拷贝）
func (fe *FieldExtractor) ExtractFieldData(fieldName string, fieldType FieldType) ([]byte, error) {
	// 重置位置
	fe.pos = 0

	// 解析map头部
	mapLen, err := fe.parseMapHeader()
	if err != nil {
		return nil, fmt.Errorf("failed to parse map header: %w", err)
	}

	// 遍历map查找目标字段
	for i := 0; i < mapLen; i++ {
		// 读取key
		key, err := fe.readString()
		if err != nil {
			return nil, fmt.Errorf("failed to read key: %w", err)
		}

		// 如果key匹配，读取value
		if key == fieldName {
			// 找到目标字段，读取其原始字节（零拷贝）
			valueBytes, err := fe.readValueBytesZeroCopy()
			if err != nil {
				return nil, fmt.Errorf("failed to read value bytes for field %s: %w", fieldName, err)
			}

			// 根据字段类型验证和返回数据
			return fe.validateAndConvert(valueBytes, fieldType)
		}

		// 跳过value
		if err := fe.skipValue(); err != nil {
			return nil, fmt.Errorf("failed to skip value: %w", err)
		}
	}

	return nil, fmt.Errorf("field %s not found", fieldName)
}

// parseMapHeader 解析map头部，返回map的长度
func (fe *FieldExtractor) parseMapHeader() (int, error) {
	if fe.pos >= len(fe.originalData) {
		return 0, fmt.Errorf("no data to parse")
	}

	typeCode := fe.originalData[fe.pos]
	fe.pos++

	switch {
	case typeCode >= msgpcode.FixedMapLow && typeCode <= msgpcode.FixedMapHigh:
		// fixmap: 长度在类型码中
		return int(typeCode & 0x0f), nil

	case typeCode == msgpcode.Map16:
		// map16: 2字节长度
		if fe.pos+2 > len(fe.originalData) {
			return 0, fmt.Errorf("invalid map16 data")
		}
		length := int(binary.BigEndian.Uint16(fe.originalData[fe.pos : fe.pos+2]))
		fe.pos += 2
		return length, nil

	case typeCode == msgpcode.Map32:
		// map32: 4字节长度
		if fe.pos+4 > len(fe.originalData) {
			return 0, fmt.Errorf("invalid map32 data")
		}
		length := int(binary.BigEndian.Uint32(fe.originalData[fe.pos : fe.pos+4]))
		fe.pos += 4
		return length, nil

	default:
		return 0, fmt.Errorf("not a map type: 0x%02x", typeCode)
	}
}

// readString 读取字符串（零拷贝）
func (fe *FieldExtractor) readString() (string, error) {
	if fe.pos >= len(fe.originalData) {
		return "", fmt.Errorf("no data to read string")
	}

	typeCode := fe.originalData[fe.pos]
	fe.pos++

	var strData []byte

	switch {
	case typeCode >= msgpcode.FixedStrLow && typeCode <= msgpcode.FixedStrHigh:
		// 固定长度字符串
		strLen := int(typeCode & 0x1f)
		if fe.pos+strLen > len(fe.originalData) {
			return "", fmt.Errorf("invalid fixed string length")
		}
		strData = fe.originalData[fe.pos : fe.pos+strLen : fe.pos+strLen]
		fe.pos += strLen

	case typeCode == msgpcode.Str8:
		// str8
		if fe.pos+1 > len(fe.originalData) {
			return "", fmt.Errorf("invalid str8 length")
		}
		strLen := int(fe.originalData[fe.pos])
		fe.pos++
		if fe.pos+strLen > len(fe.originalData) {
			return "", fmt.Errorf("invalid str8 data")
		}
		strData = fe.originalData[fe.pos : fe.pos+strLen : fe.pos+strLen]
		fe.pos += strLen

	case typeCode == msgpcode.Str16:
		// str16
		if fe.pos+2 > len(fe.originalData) {
			return "", fmt.Errorf("invalid str16 length")
		}
		strLen := int(binary.BigEndian.Uint16(fe.originalData[fe.pos : fe.pos+2]))
		fe.pos += 2
		if fe.pos+strLen > len(fe.originalData) {
			return "", fmt.Errorf("invalid str16 data")
		}
		strData = fe.originalData[fe.pos : fe.pos+strLen : fe.pos+strLen]
		fe.pos += strLen

	case typeCode == msgpcode.Str32:
		// str32
		if fe.pos+4 > len(fe.originalData) {
			return "", fmt.Errorf("invalid str32 length")
		}
		strLen := int(binary.BigEndian.Uint32(fe.originalData[fe.pos : fe.pos+4]))
		fe.pos += 4
		if fe.pos+strLen > len(fe.originalData) {
			return "", fmt.Errorf("invalid str32 data")
		}
		strData = fe.originalData[fe.pos : fe.pos+strLen : fe.pos+strLen]
		fe.pos += strLen

	default:
		return "", fmt.Errorf("not a string type: 0x%02x", typeCode)
	}

	return string(strData), nil
}

// readValueBytesZeroCopy 读取value的原始字节数据（真正的零拷贝）
// 直接基于原始MessagePack数据创建切片，不进行额外的内存分配
func (fe *FieldExtractor) readValueBytesZeroCopy() ([]byte, error) {
	if fe.pos >= len(fe.originalData) {
		return nil, fmt.Errorf("no data to read")
	}

	// 保存起始位置
	startPos := fe.pos

	// 读取类型码
	typeCode := fe.originalData[fe.pos]
	fe.pos++

	// 根据类型码确定数据的长度和移动pos
	var data []byte

	switch {
	case typeCode <= msgpcode.PosFixedNumHigh:
		// 正整数 (0-127): 类型码就是数据本身
		data = fe.originalData[startPos : startPos+1 : startPos+1]

	case typeCode >= msgpcode.NegFixedNumLow:
		// 负整数 (-32到-1): 类型码就是数据本身
		data = fe.originalData[startPos : startPos+1 : startPos+1]

	case typeCode == msgpcode.Uint8:
		// uint8: 1字节类型码 + 1字节数据
		if fe.pos+1 > len(fe.originalData) {
			return nil, fmt.Errorf("invalid uint8 data length")
		}
		data = fe.originalData[startPos+1 : startPos+2 : startPos+2]
		fe.pos += 1

	case typeCode == msgpcode.Uint16:
		// uint16: 1字节类型码 + 2字节数据
		if fe.pos+2 > len(fe.originalData) {
			return nil, fmt.Errorf("invalid uint16 data length")
		}
		data = fe.originalData[startPos+1 : startPos+3 : startPos+3]
		fe.pos += 2

	case typeCode == msgpcode.Uint32:
		// uint32: 1字节类型码 + 4字节数据
		if fe.pos+4 > len(fe.originalData) {
			return nil, fmt.Errorf("invalid uint32 data length")
		}
		data = fe.originalData[startPos+1 : startPos+5 : startPos+5]
		fe.pos += 4

	case typeCode == msgpcode.Uint64:
		// uint64: 1字节类型码 + 8字节数据
		if fe.pos+8 > len(fe.originalData) {
			return nil, fmt.Errorf("invalid uint64 data length")
		}
		data = fe.originalData[startPos+1 : startPos+9 : startPos+9]
		fe.pos += 8

	case typeCode == msgpcode.Int8:
		// int8: 1字节类型码 + 1字节数据
		if fe.pos+1 > len(fe.originalData) {
			return nil, fmt.Errorf("invalid int8 data length")
		}
		data = fe.originalData[startPos+1 : startPos+2 : startPos+2]
		fe.pos += 1

	case typeCode == msgpcode.Int16:
		// int16: 1字节类型码 + 2字节数据
		if fe.pos+2 > len(fe.originalData) {
			return nil, fmt.Errorf("invalid int16 data length")
		}
		data = fe.originalData[startPos+1 : startPos+3 : startPos+3]
		fe.pos += 2

	case typeCode == msgpcode.Int32:
		// int32: 1字节类型码 + 4字节数据
		if fe.pos+4 > len(fe.originalData) {
			return nil, fmt.Errorf("invalid int32 data length")
		}
		data = fe.originalData[startPos+1 : startPos+5 : startPos+5]
		fe.pos += 4

	case typeCode == msgpcode.Int64:
		// int64: 1字节类型码 + 8字节数据
		if fe.pos+8 > len(fe.originalData) {
			return nil, fmt.Errorf("invalid int64 data length")
		}
		data = fe.originalData[startPos+1 : startPos+9 : startPos+9]
		fe.pos += 8

	case typeCode == msgpcode.Float:
		// float32: 1字节类型码 + 4字节数据
		if fe.pos+4 > len(fe.originalData) {
			return nil, fmt.Errorf("invalid float32 data length")
		}
		data = fe.originalData[startPos+1 : startPos+5 : startPos+5]
		fe.pos += 4

	case typeCode == msgpcode.Double:
		// float64: 1字节类型码 + 8字节数据
		if fe.pos+8 > len(fe.originalData) {
			return nil, fmt.Errorf("invalid float64 data length")
		}
		data = fe.originalData[startPos+1 : startPos+9 : startPos+9]
		fe.pos += 8

	case typeCode == msgpcode.Nil:
		// nil: 只有类型码
		return nil, nil

	case typeCode == msgpcode.True:
		// true: 只有类型码
		data = fe.originalData[startPos : startPos+1 : startPos+1]

	case typeCode == msgpcode.False:
		// false: 只有类型码
		data = fe.originalData[startPos : startPos+1 : startPos+1]

	case typeCode >= msgpcode.FixedStrLow && typeCode <= msgpcode.FixedStrHigh:
		// 固定长度字符串: 类型码包含长度信息
		strLen := int(typeCode & 0x1f)
		if fe.pos+strLen > len(fe.originalData) {
			return nil, fmt.Errorf("invalid fixed string data length")
		}
		data = fe.originalData[startPos+1 : startPos+1+strLen : startPos+1+strLen]
		fe.pos += strLen

	case typeCode == msgpcode.Str8:
		// str8: 1字节类型码 + 1字节长度 + 数据
		if fe.pos+1 > len(fe.originalData) {
			return nil, fmt.Errorf("invalid str8 data length")
		}
		strLen := int(fe.originalData[fe.pos])
		fe.pos++
		if fe.pos+strLen > len(fe.originalData) {
			return nil, fmt.Errorf("invalid str8 data")
		}
		data = fe.originalData[startPos+2 : startPos+2+strLen : startPos+2+strLen]
		fe.pos += strLen

	case typeCode == msgpcode.Str16:
		// str16: 1字节类型码 + 2字节长度 + 数据
		if fe.pos+2 > len(fe.originalData) {
			return nil, fmt.Errorf("invalid str16 data length")
		}
		strLen := int(binary.BigEndian.Uint16(fe.originalData[fe.pos : fe.pos+2]))
		fe.pos += 2
		if fe.pos+strLen > len(fe.originalData) {
			return nil, fmt.Errorf("invalid str16 data")
		}
		data = fe.originalData[startPos+3 : startPos+3+strLen : startPos+3+strLen]
		fe.pos += strLen

	case typeCode == msgpcode.Str32:
		// str32: 1字节类型码 + 4字节长度 + 数据
		if fe.pos+4 > len(fe.originalData) {
			return nil, fmt.Errorf("invalid str32 data length")
		}
		strLen := int(binary.BigEndian.Uint32(fe.originalData[fe.pos : fe.pos+4]))
		fe.pos += 4
		if fe.pos+strLen > len(fe.originalData) {
			return nil, fmt.Errorf("invalid str32 data")
		}
		data = fe.originalData[startPos+5 : startPos+5+strLen : startPos+5+strLen]
		fe.pos += strLen

	case typeCode == msgpcode.Bin8:
		// bin8: 1字节类型码 + 1字节长度 + 数据
		if fe.pos+1 > len(fe.originalData) {
			return nil, fmt.Errorf("invalid bin8 data length")
		}
		binLen := int(fe.originalData[fe.pos])
		fe.pos++
		if fe.pos+binLen > len(fe.originalData) {
			return nil, fmt.Errorf("invalid bin8 data")
		}
		data = fe.originalData[startPos+2 : startPos+2+binLen : startPos+2+binLen]
		fe.pos += binLen

	case typeCode == msgpcode.Bin16:
		// bin16: 1字节类型码 + 2字节长度 + 数据
		if fe.pos+2 > len(fe.originalData) {
			return nil, fmt.Errorf("invalid bin16 data length")
		}
		binLen := int(binary.BigEndian.Uint16(fe.originalData[fe.pos : fe.pos+2]))
		fe.pos += 2
		if fe.pos+binLen > len(fe.originalData) {
			return nil, fmt.Errorf("invalid bin16 data")
		}
		data = fe.originalData[startPos+3 : startPos+3+binLen : startPos+3+binLen]
		fe.pos += binLen

	case typeCode == msgpcode.Bin32:
		// bin32: 1字节类型码 + 4字节长度 + 数据
		if fe.pos+4 > len(fe.originalData) {
			return nil, fmt.Errorf("invalid bin32 data length")
		}
		binLen := int(binary.BigEndian.Uint32(fe.originalData[fe.pos : fe.pos+4]))
		fe.pos += 4
		if fe.pos+binLen > len(fe.originalData) {
			return nil, fmt.Errorf("invalid bin32 data")
		}
		data = fe.originalData[startPos+5 : startPos+5+binLen : startPos+5+binLen]
		fe.pos += binLen

	default:
		// 对于其他复杂类型（数组、map等），返回整个值（包括类型码）
		// 并跳过整个值
		endPos, err := fe.skipValueInternal()
		if err != nil {
			return nil, err
		}
		data = fe.originalData[startPos:endPos:endPos]
	}

	return data, nil
}

// skipValueInternal 跳过当前值，返回结束位置
func (fe *FieldExtractor) skipValueInternal() (int, error) {
	if fe.pos >= len(fe.originalData) {
		return fe.pos, fmt.Errorf("no data to skip")
	}

	typeCode := fe.originalData[fe.pos]
	fe.pos++

	switch {
	case typeCode <= msgpcode.PosFixedNumHigh:
		// 正整数: 不需要跳过额外数据

	case typeCode >= msgpcode.NegFixedNumLow:
		// 负整数: 不需要跳过额外数据

	case typeCode == msgpcode.Uint8:
		// uint8: 1字节数据
		fe.pos += 1

	case typeCode == msgpcode.Uint16:
		// uint16: 2字节数据
		fe.pos += 2

	case typeCode == msgpcode.Uint32:
		// uint32: 4字节数据
		fe.pos += 4

	case typeCode == msgpcode.Uint64:
		// uint64: 8字节数据
		fe.pos += 8

	case typeCode == msgpcode.Int8:
		// int8: 1字节数据
		fe.pos += 1

	case typeCode == msgpcode.Int16:
		// int16: 2字节数据
		fe.pos += 2

	case typeCode == msgpcode.Int32:
		// int32: 4字节数据
		fe.pos += 4

	case typeCode == msgpcode.Int64:
		// int64: 8字节数据
		fe.pos += 8

	case typeCode == msgpcode.Float:
		// float32: 4字节数据
		fe.pos += 4

	case typeCode == msgpcode.Double:
		// float64: 8字节数据
		fe.pos += 8

	case typeCode == msgpcode.Nil, typeCode == msgpcode.True, typeCode == msgpcode.False:
		// 不需要跳过额外数据

	case typeCode >= msgpcode.FixedStrLow && typeCode <= msgpcode.FixedStrHigh:
		// 固定长度字符串
		strLen := int(typeCode & 0x1f)
		fe.pos += strLen

	case typeCode == msgpcode.Str8:
		// str8
		if fe.pos+1 > len(fe.originalData) {
			return fe.pos, fmt.Errorf("invalid str8 length")
		}
		strLen := int(fe.originalData[fe.pos])
		fe.pos += 1 + strLen

	case typeCode == msgpcode.Str16:
		// str16
		if fe.pos+2 > len(fe.originalData) {
			return fe.pos, fmt.Errorf("invalid str16 length")
		}
		strLen := int(binary.BigEndian.Uint16(fe.originalData[fe.pos : fe.pos+2]))
		fe.pos += 2 + strLen

	case typeCode == msgpcode.Str32:
		// str32
		if fe.pos+4 > len(fe.originalData) {
			return fe.pos, fmt.Errorf("invalid str32 length")
		}
		strLen := int(binary.BigEndian.Uint32(fe.originalData[fe.pos : fe.pos+4]))
		fe.pos += 4 + strLen

	case typeCode == msgpcode.Bin8:
		// bin8
		if fe.pos+1 > len(fe.originalData) {
			return fe.pos, fmt.Errorf("invalid bin8 length")
		}
		binLen := int(fe.originalData[fe.pos])
		fe.pos += 1 + binLen

	case typeCode == msgpcode.Bin16:
		// bin16
		if fe.pos+2 > len(fe.originalData) {
			return fe.pos, fmt.Errorf("invalid bin16 length")
		}
		binLen := int(binary.BigEndian.Uint16(fe.originalData[fe.pos : fe.pos+2]))
		fe.pos += 2 + binLen

	case typeCode == msgpcode.Bin32:
		// bin32
		if fe.pos+4 > len(fe.originalData) {
			return fe.pos, fmt.Errorf("invalid bin32 length")
		}
		binLen := int(binary.BigEndian.Uint32(fe.originalData[fe.pos : fe.pos+4]))
		fe.pos += 4 + binLen

	case typeCode >= msgpcode.FixedArrayLow && typeCode <= msgpcode.FixedArrayHigh:
		// fixarray
		arrayLen := int(typeCode & 0x0f)
		for i := 0; i < arrayLen; i++ {
			if _, err := fe.skipValueInternal(); err != nil {
				return fe.pos, err
			}
		}

	case typeCode == msgpcode.Array16:
		// array16
		if fe.pos+2 > len(fe.originalData) {
			return fe.pos, fmt.Errorf("invalid array16 length")
		}
		arrayLen := int(binary.BigEndian.Uint16(fe.originalData[fe.pos : fe.pos+2]))
		fe.pos += 2
		for i := 0; i < arrayLen; i++ {
			if _, err := fe.skipValueInternal(); err != nil {
				return fe.pos, err
			}
		}

	case typeCode == msgpcode.Array32:
		// array32
		if fe.pos+4 > len(fe.originalData) {
			return fe.pos, fmt.Errorf("invalid array32 length")
		}
		arrayLen := int(binary.BigEndian.Uint32(fe.originalData[fe.pos : fe.pos+4]))
		fe.pos += 4
		for i := 0; i < arrayLen; i++ {
			if _, err := fe.skipValueInternal(); err != nil {
				return fe.pos, err
			}
		}

	case typeCode >= msgpcode.FixedMapLow && typeCode <= msgpcode.FixedMapHigh:
		// fixmap
		mapLen := int(typeCode & 0x0f)
		for i := 0; i < mapLen; i++ {
			// 跳过key
			if _, err := fe.skipValueInternal(); err != nil {
				return fe.pos, err
			}
			// 跳过value
			if _, err := fe.skipValueInternal(); err != nil {
				return fe.pos, err
			}
		}

	case typeCode == msgpcode.Map16:
		// map16
		if fe.pos+2 > len(fe.originalData) {
			return fe.pos, fmt.Errorf("invalid map16 length")
		}
		mapLen := int(binary.BigEndian.Uint16(fe.originalData[fe.pos : fe.pos+2]))
		fe.pos += 2
		for i := 0; i < mapLen; i++ {
			// 跳过key
			if _, err := fe.skipValueInternal(); err != nil {
				return fe.pos, err
			}
			// 跳过value
			if _, err := fe.skipValueInternal(); err != nil {
				return fe.pos, err
			}
		}

	case typeCode == msgpcode.Map32:
		// map32
		if fe.pos+4 > len(fe.originalData) {
			return fe.pos, fmt.Errorf("invalid map32 length")
		}
		mapLen := int(binary.BigEndian.Uint32(fe.originalData[fe.pos : fe.pos+4]))
		fe.pos += 4
		for i := 0; i < mapLen; i++ {
			// 跳过key
			if _, err := fe.skipValueInternal(); err != nil {
				return fe.pos, err
			}
			// 跳过value
			if _, err := fe.skipValueInternal(); err != nil {
				return fe.pos, err
			}
		}

	default:
		return fe.pos, fmt.Errorf("unknown type code: 0x%02x", typeCode)
	}

	return fe.pos, nil
}

// skipValue 跳过当前值
func (fe *FieldExtractor) skipValue() error {
	_, err := fe.skipValueInternal()
	return err
}

// validateAndConvert 验证并转换字段数据
func (fe *FieldExtractor) validateAndConvert(data []byte, fieldType FieldType) ([]byte, error) {
	if data == nil {
		return nil, nil
	}

	// 检查数据长度是否符合类型要求
	switch fieldType {
	case FieldTypeInt8, FieldTypeUInt8, FieldTypeBool:
		if len(data) < 1 {
			return nil, fmt.Errorf("data too short for type %s, expected at least 1 byte, got %d", fieldType, len(data))
		}
	case FieldTypeInt16, FieldTypeUInt16:
		if len(data) < 2 {
			return nil, fmt.Errorf("data too short for type %s, expected at least 2 bytes, got %d", fieldType, len(data))
		}
	case FieldTypeInt32, FieldTypeUInt32, FieldTypeFloat32:
		if len(data) < 4 {
			return nil, fmt.Errorf("data too short for type %s, expected at least 4 bytes, got %d", fieldType, len(data))
		}
	case FieldTypeInt64, FieldTypeUInt64, FieldTypeFloat64, FieldTypeDateTime:
		if len(data) < 8 {
			return nil, fmt.Errorf("data too short for type %s, expected at least 8 bytes, got %d", fieldType, len(data))
		}
	case FieldTypeIPv4:
		if len(data) < 4 {
			return nil, fmt.Errorf("data too short for type %s, expected at least 4 bytes, got %d", fieldType, len(data))
		}
	case FieldTypeIPv6:
		if len(data) < 16 {
			return nil, fmt.Errorf("data too short for type %s, expected at least 16 bytes, got %d", fieldType, len(data))
		}
	case FieldTypeUUID:
		if len(data) < 16 {
			return nil, fmt.Errorf("data too short for type %s, expected at least 16 bytes, got %d", fieldType, len(data))
		}
	case FieldTypeString:
		// 字符串类型可以是任意长度
		return data, nil
	case FieldTypeDate:
		if len(data) < 4 {
			return nil, fmt.Errorf("data too short for type %s, expected at least 4 bytes, got %d", fieldType, len(data))
		}
	default:
		return nil, fmt.Errorf("unsupported field type: %s", fieldType)
	}

	return data, nil
}

// ExtractAllFields 提取映射配置中所有字段的数据（单个记录）
// 返回 FieldBatch 对象，行数为 1
func ExtractAllFields(data []byte, mapping *MappingConfig) (*model.FieldBatch, error) {
	extractor := NewFieldExtractor(data)
	result := make(map[string][]byte)

	// 解析map头部
	mapLen, err := extractor.parseMapHeader()
	if err != nil {
		return nil, fmt.Errorf("failed to parse map header: %w", err)
	}

	// 创建需要的字段集合
	requiredFields := make(map[string]bool)
	for _, field := range mapping.Fields {
		requiredFields[field.MsgPackFieldName] = true
	}

	// 遍历map，提取所有需要的字段
	for i := 0; i < mapLen; i++ {
		// 读取key
		key, err := extractor.readString()
		if err != nil {
			return nil, fmt.Errorf("failed to read key: %w", err)
		}

		// 检查是否是我们需要的字段
		if requiredFields[key] {
			// 找到需要的字段，提取其数据
			fieldData, err := extractor.readValueBytesZeroCopy()
			if err != nil {
				return nil, fmt.Errorf("failed to read value for field %s: %w", key, err)
			}
			result[key] = fieldData

			// 从需要的字段集合中移除
			delete(requiredFields, key)

			// 如果所有字段都已找到，可以提前退出
			if len(requiredFields) == 0 {
				break
			}
		} else {
			// 跳过不需要的value
			if err := extractor.skipValue(); err != nil {
				return nil, fmt.Errorf("failed to skip value: %w", err)
			}
		}
	}

	// 检查是否所有需要的字段都已找到
	if len(requiredFields) > 0 {
		missingFields := make([]string, 0, len(requiredFields))
		for field := range requiredFields {
			missingFields = append(missingFields, field)
		}
		return nil, fmt.Errorf("required fields not found: %v", missingFields)
	}

	// 将单个记录转换为 FieldBatch（行数为 1）
	batchData := make(map[string][][]byte)
	for fieldName, data := range result {
		batchData[fieldName] = [][]byte{data}
	}

	return model.NewFieldBatch(batchData), nil
}

// ExtractAllFieldsLegacy 提取映射配置中所有字段的数据（旧版接口，保持向后兼容）
// 返回字段名到字节数据的映射
func ExtractAllFieldsLegacy(data []byte, mapping *MappingConfig) (map[string][]byte, error) {
	batch, err := ExtractAllFields(data, mapping)
	if err != nil {
		return nil, err
	}

	// 从 FieldBatch 提取第一个元素
	result := make(map[string][]byte)
	batchData := batch.GetData()
	for fieldName, values := range batchData {
		if len(values) > 0 && values[0] != nil {
			result[fieldName] = values[0]
		} else {
			result[fieldName] = nil
		}
	}

	return result, nil
}

// ExtractAllFieldsBatch 批量提取字段数据，支持从MessagePack数组中提取多个记录
// 支持两种批量数据格式：
// 1. 行式存储（Row-based）：[{"user_id": 1, "event_type": "login"}, {"user_id": 2, "event_type": "logout"}]
// 2. 列式存储（Column-based）：{"user_id": [1, 2], "event_type": ["login", "logout"]}
//
// 返回 FieldBatch 结构体，包含字段数据、行数统计和字节大小计算
// 注意：确保所有字段的切片长度一致，对于缺失的字段使用nil
func ExtractAllFieldsBatch(data []byte, mapping *MappingConfig) (*model.FieldBatch, error) {
	extractor := NewFieldExtractor(data)

	// 检测数据类型
	if extractor.pos >= len(extractor.originalData) {
		return nil, fmt.Errorf("no data to parse")
	}

	typeCode := extractor.originalData[extractor.pos]

	// 检测是否为数组类型（行式存储）
	if typeCode >= msgpcode.FixedArrayLow && typeCode <= msgpcode.FixedArrayHigh ||
		typeCode == msgpcode.Array16 || typeCode == msgpcode.Array32 {

		return extractRowBasedBatch(extractor, mapping)
	}

	// 检测是否为map类型（可能是列式存储或单个记录）
	if typeCode >= msgpcode.FixedMapLow && typeCode <= msgpcode.FixedMapHigh ||
		typeCode == msgpcode.Map16 || typeCode == msgpcode.Map32 {

		// 尝试解析为列式存储
		result, isColumnar, err := extractColumnBasedBatch(extractor, mapping)
		if err != nil {
			return nil, err
		}

		if isColumnar {
			return result, nil
		}

		// 不是列式存储，当作单个记录处理
		return convertSingleRecordToBatch(result, mapping)
	}

	// 未知格式
	return nil, fmt.Errorf("unsupported MessagePack format for batch data")
}

// ExtractAllFieldsBatchLegacy 批量提取字段数据（旧版接口，保持向后兼容）
// 返回字段名到字节数组切片的映射
func ExtractAllFieldsBatchLegacy(data []byte, mapping *MappingConfig) (map[string][][]byte, error) {
	batch, err := ExtractAllFieldsBatch(data, mapping)
	if err != nil {
		return nil, err
	}
	return batch.GetData(), nil
}

// extractRowBasedBatch 提取行式存储的批量数据
// 格式：[{"user_id": 1, "event_type": "login"}, {"user_id": 2, "event_type": "logout"}]
func extractRowBasedBatch(extractor *FieldExtractor, mapping *MappingConfig) (*model.FieldBatch, error) {
	// 解析数组
	arrayLen, err := extractor.parseArrayHeader()
	if err != nil {
		return nil, fmt.Errorf("failed to parse array header: %w", err)
	}

	// 初始化结果：每个字段对应一个切片，预分配长度
	result := make(map[string][][]byte)
	for _, field := range mapping.Fields {
		result[field.MsgPackFieldName] = make([][]byte, arrayLen)
	}

	// 遍历数组中的每个元素（每个元素应该是一个map）
	for i := 0; i < arrayLen; i++ {
		// 提取当前记录的字段数据
		recordFields, err := extractor.extractRecordFields(mapping)
		if err != nil {
			return nil, fmt.Errorf("failed to extract record fields at index %d: %w", i, err)
		}

		// 将字段数据添加到结果中
		// 对于每个映射配置中定义的字段，检查记录中是否有该字段
		for _, field := range mapping.Fields {
			fieldName := field.MsgPackFieldName
			if fieldData, exists := recordFields[fieldName]; exists {
				// 记录中有该字段，使用提取的数据
				result[fieldName][i] = fieldData
			} else {
				// 记录中缺少该字段，使用nil
				result[fieldName][i] = nil
			}
		}
	}

	return model.NewFieldBatch(result), nil
}

// extractColumnBasedBatch 提取列式存储的批量数据
// 格式：{"user_id": [1, 2], "event_type": ["login", "logout"]}
// 返回：(结果数据, 是否为列式存储, 错误)
func extractColumnBasedBatch(extractor *FieldExtractor, mapping *MappingConfig) (*model.FieldBatch, bool, error) {
	// 解析map头部
	mapLen, err := extractor.parseMapHeader()
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse map header: %w", err)
	}

	// 临时存储所有字段的数据
	tempResult := make(map[string][]byte)

	// 遍历map，提取所有字段
	for i := 0; i < mapLen; i++ {
		// 读取key
		key, err := extractor.readString()
		if err != nil {
			return nil, false, fmt.Errorf("failed to read key: %w", err)
		}

		// 读取value
		valueBytes, err := extractor.readValueBytesZeroCopy()
		if err != nil {
			return nil, false, fmt.Errorf("failed to read value for field %s: %w", key, err)
		}

		tempResult[key] = valueBytes
	}

	// 检查是否为列式存储
	// 列式存储的特征：所有值都是数组类型
	isColumnar := true
	var rowCount int = -1

	for fieldName, valueBytes := range tempResult {
		if valueBytes == nil {
			continue
		}

		// 检查是否为数组类型
		typeCode := valueBytes[0]
		isArray := (typeCode >= msgpcode.FixedArrayLow && typeCode <= msgpcode.FixedArrayHigh) ||
			typeCode == msgpcode.Array16 || typeCode == msgpcode.Array32

		if !isArray {
			// 不是数组，说明是行式存储的单个记录
			isColumnar = false
			break
		}

		// 如果是数组，解析数组长度
		arrayExtractor := NewFieldExtractor(valueBytes)
		currentRowCount, err := arrayExtractor.parseArrayHeader()
		if err != nil {
			return nil, false, fmt.Errorf("failed to parse array for field %s: %w", fieldName, err)
		}

		if rowCount == -1 {
			rowCount = currentRowCount
		} else if rowCount != currentRowCount {
			// 数组长度不一致，可能是行式存储的单个记录
			isColumnar = false
			break
		}
	}

	if !isColumnar || rowCount == -1 {
		// 不是列式存储，返回原始数据
		result := make(map[string][][]byte)
		for fieldName, valueBytes := range tempResult {
			result[fieldName] = [][]byte{valueBytes}
		}
		return model.NewFieldBatch(result), false, nil
	}

	// 确认是列式存储，提取所有字段的数据
	result := make(map[string][][]byte)
	for _, field := range mapping.Fields {
		fieldName := field.MsgPackFieldName
		if valueBytes, exists := tempResult[fieldName]; exists && valueBytes != nil {
			// 提取数组中的每个元素
			arrayExtractor := NewFieldExtractor(valueBytes)
			arrayLen, err := arrayExtractor.parseArrayHeader()
			if err != nil {
				return nil, false, fmt.Errorf("failed to parse array for field %s: %w", fieldName, err)
			}

			fieldValues := make([][]byte, arrayLen)
			for j := 0; j < arrayLen; j++ {
				valueData, err := arrayExtractor.readValueBytesZeroCopy()
				if err != nil {
					return nil, false, fmt.Errorf("failed to read array element %d for field %s: %w", j, fieldName, err)
				}
				fieldValues[j] = valueData
			}
			result[fieldName] = fieldValues
		} else {
			// 字段不存在，填充nil
			result[fieldName] = make([][]byte, rowCount)
			for j := 0; j < rowCount; j++ {
				result[fieldName][j] = nil
			}
		}
	}

	return model.NewFieldBatch(result), true, nil
}

// convertSingleRecordToBatch 将单个记录转换为批量格式
func convertSingleRecordToBatch(singleRecord *model.FieldBatch, mapping *MappingConfig) (*model.FieldBatch, error) {
	result := make(map[string][][]byte)
	for _, field := range mapping.Fields {
		fieldName := field.MsgPackFieldName
		if fieldDataSlice := singleRecord.GetField(fieldName); len(fieldDataSlice) > 0 {
			result[fieldName] = [][]byte{fieldDataSlice[0]}
		} else {
			result[fieldName] = [][]byte{nil}
		}
	}
	return model.NewFieldBatch(result), nil
}

// parseArrayHeader 解析数组头部，返回数组长度
func (fe *FieldExtractor) parseArrayHeader() (int, error) {
	if fe.pos >= len(fe.originalData) {
		return 0, fmt.Errorf("no data to parse")
	}

	typeCode := fe.originalData[fe.pos]
	fe.pos++

	switch {
	case typeCode >= msgpcode.FixedArrayLow && typeCode <= msgpcode.FixedArrayHigh:
		// fixarray: 长度在类型码中
		return int(typeCode & 0x0f), nil

	case typeCode == msgpcode.Array16:
		// array16: 2字节长度
		if fe.pos+2 > len(fe.originalData) {
			return 0, fmt.Errorf("invalid array16 data")
		}
		length := int(binary.BigEndian.Uint16(fe.originalData[fe.pos : fe.pos+2]))
		fe.pos += 2
		return length, nil

	case typeCode == msgpcode.Array32:
		// array32: 4字节长度
		if fe.pos+4 > len(fe.originalData) {
			return 0, fmt.Errorf("invalid array32 data")
		}
		length := int(binary.BigEndian.Uint32(fe.originalData[fe.pos : fe.pos+4]))
		fe.pos += 4
		return length, nil

	default:
		return 0, fmt.Errorf("not an array type: 0x%02x", typeCode)
	}
}

// extractRecordFields 提取单个记录的字段数据
func (fe *FieldExtractor) extractRecordFields(mapping *MappingConfig) (map[string][]byte, error) {
	// 解析map头部
	mapLen, err := fe.parseMapHeader()
	if err != nil {
		return nil, fmt.Errorf("failed to parse map header: %w", err)
	}

	// 创建需要的字段集合
	requiredFields := make(map[string]bool)
	for _, field := range mapping.Fields {
		requiredFields[field.MsgPackFieldName] = true
	}

	result := make(map[string][]byte)

	// 遍历map，提取所有需要的字段
	for i := 0; i < mapLen; i++ {
		// 读取key
		key, err := fe.readString()
		if err != nil {
			return nil, fmt.Errorf("failed to read key: %w", err)
		}

		// 检查是否是我们需要的字段
		if requiredFields[key] {
			// 找到需要的字段，提取其数据
			fieldData, err := fe.readValueBytesZeroCopy()
			if err != nil {
				return nil, fmt.Errorf("failed to read value for field %s: %w", key, err)
			}
			result[key] = fieldData

			// 从需要的字段集合中移除
			delete(requiredFields, key)

			// 如果所有字段都已找到，可以提前退出
			if len(requiredFields) == 0 {
				break
			}
		} else {
			// 跳过不需要的value
			if err := fe.skipValue(); err != nil {
				return nil, fmt.Errorf("failed to skip value: %w", err)
			}
		}
	}

	// 检查是否所有需要的字段都已找到
	if len(requiredFields) > 0 {
		missingFields := make([]string, 0, len(requiredFields))
		for field := range requiredFields {
			missingFields = append(missingFields, field)
		}
		return nil, fmt.Errorf("required fields not found: %v", missingFields)
	}

	return result, nil
}
