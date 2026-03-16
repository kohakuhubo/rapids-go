# ClickHouse 列写入器

此包提供了 `ColumnWriter` 工具，用于将各种数据类型写入 ClickHouse 列，特别支持字节数据和不安全指针转换。

## 功能特性

- 写入所有主要的 ClickHouse 数据类型
- 支持直接将字节数据写入列
- 从字节数组到原生类型的不安全指针转换
- 类型安全的列创建
- 高效的批量插入

## 使用方法

### 基本使用

```go
import (
    "berry-rapids-go/pkg/clickhouse"
    "github.com/ClickHouse/ch-go/proto"
)

// 创建一个新的 ColumnWriter 实例
writer := clickhouse.NewColumnWriter()

// 写入字符串数据
stringCol := writer.WriteStringColumn([]string{"value1", "value2", "value3"})

// 直接将字节数据作为字符串写入
byteCol := writer.WriteStringColumnFromBytes([][]byte{
    []byte(`{"json": "data"}`),
    []byte{0x01, 0x02, 0x03, 0x04},
})

// 写入整数数据
intCol := writer.WriteUInt64Column([]uint64{1, 2, 3})

// 写入日期/时间数据
timeCol := writer.WriteDateTimeColumn([]time.Time{time.Now()})

// 创建插入的输入块
input := proto.Input{
    proto.InputColumn{Name: "string_data", Data: stringCol},
    proto.InputColumn{Name: "byte_data", Data: byteCol},
    proto.InputColumn{Name: "int_data", Data: intCol},
    proto.InputColumn{Name: "time_data", Data: timeCol},
}
```

### 不安全指针使用

```go
// 使用不安全指针转换从字节切片写入数值数据
uint32Bytes := []byte{0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00} // 表示 [1, 2]
uint32Col := writer.WriteUInt32ColumnFromBytes(uint32Bytes)

float64Bytes := make([]byte, 16) // 两个 float64 值的空间
// 填充 float64 值的字节表示
float64Col := writer.WriteFloat64ColumnFromBytes(float64Bytes)

// 创建插入的输入块
input := proto.Input{
    proto.InputColumn{Name: "uint32_data", Data: uint32Col},
    proto.InputColumn{Name: "float64_data", Data: float64Col},
}
```

## 支持的数据类型

### 字符串类型
- **String**: `WriteStringColumn`, `WriteStringColumnFromBytes`
- **FixedString**: `WriteFixedStringColumn`

### 无符号整数类型
- **UInt8**: `WriteUInt8Column`, `WriteUInt8ColumnFromBytes`
- **UInt16**: `WriteUInt16Column`, `WriteUInt16ColumnFromBytes`
- **UInt32**: `WriteUInt32Column`, `WriteUInt32ColumnFromBytes`
- **UInt64**: `WriteUInt64Column`, `WriteUInt64ColumnFromBytes`

### 有符号整数类型
- **Int8**: `WriteInt8Column`, `WriteInt8ColumnFromBytes`
- **Int16**: `WriteInt16Column`, `WriteInt16ColumnFromBytes`
- **Int32**: `WriteInt32Column`, `WriteInt32ColumnFromBytes`
- **Int64**: `WriteInt64Column`, `WriteInt64ColumnFromBytes`

### 浮点类型
- **Float32**: `WriteFloat32Column`, `WriteFloat32ColumnFromBytes`
- **Float64**: `WriteFloat64Column`, `WriteFloat64ColumnFromBytes`

### 日期/时间类型
- **Date**: `WriteDateColumn`
- **DateTime**: `WriteDateTimeColumn`

### 其他类型
- **Boolean**: `WriteBoolColumn`
- **UUID**: `WriteUUIDColumn`
- **IPv4/IPv6**: `WriteIPv4Column`, `WriteIPv6Column`
- **Nullable**: `WriteNullableStringColumn`
- **Array**: `WriteArrayStringColumn`
- **Tuple**: `WriteTupleColumn`

## 主要优势

1. **性能**：直接字节到列的写入，开销最小
2. **类型安全**：数据类型的编译时类型检查
3. **内存效率**：适当的列分配和管理
4. **灵活性**：支持所有主要的 ClickHouse 数据类型
5. **不安全指针支持**：从字节数组到原生类型的高效转换