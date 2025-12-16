# ClickHouse Column Writer

This package provides a `ColumnWriter` utility for writing various data types to ClickHouse columns, with special support for byte data and unsafe pointer conversions.

## Features

- Write all major ClickHouse data types
- Support for writing byte data directly to columns
- Unsafe pointer conversion from byte arrays to native types
- Type-safe column creation
- Efficient batch insertion

## Usage

### Basic Usage

```go
import (
    "berry-rapids-go/pkg/clickhouse"
    "github.com/ClickHouse/ch-go/proto"
)

// Create a new ColumnWriter instance
writer := clickhouse.NewColumnWriter()

// Write string data
stringCol := writer.WriteStringColumn([]string{"value1", "value2", "value3"})

// Write byte data directly as strings
byteCol := writer.WriteStringColumnFromBytes([][]byte{
    []byte(`{"json": "data"}`),
    []byte{0x01, 0x02, 0x03, 0x04},
})

// Write integer data
intCol := writer.WriteUInt64Column([]uint64{1, 2, 3})

// Write date/time data
timeCol := writer.WriteDateTimeColumn([]time.Time{time.Now()})

// Create input block for insertion
input := proto.Input{
    proto.InputColumn{Name: "string_data", Data: stringCol},
    proto.InputColumn{Name: "byte_data", Data: byteCol},
    proto.InputColumn{Name: "int_data", Data: intCol},
    proto.InputColumn{Name: "time_data", Data: timeCol},
}
```

### Unsafe Pointer Usage

```go
// Write numeric data from byte slices using unsafe pointer conversion
uint32Bytes := []byte{0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00} // Represents [1, 2]
uint32Col := writer.WriteUInt32ColumnFromBytes(uint32Bytes)

float64Bytes := make([]byte, 16) // Space for two float64 values
// Fill with byte representations of float64 values
float64Col := writer.WriteFloat64ColumnFromBytes(float64Bytes)

// Create input block for insertion
input := proto.Input{
    proto.InputColumn{Name: "uint32_data", Data: uint32Col},
    proto.InputColumn{Name: "float64_data", Data: float64Col},
}
```

## Supported Data Types

### String Types
- **String**: `WriteStringColumn`, `WriteStringColumnFromBytes`
- **FixedString**: `WriteFixedStringColumn`

### Unsigned Integer Types
- **UInt8**: `WriteUInt8Column`, `WriteUInt8ColumnFromBytes`
- **UInt16**: `WriteUInt16Column`, `WriteUInt16ColumnFromBytes`
- **UInt32**: `WriteUInt32Column`, `WriteUInt32ColumnFromBytes`
- **UInt64**: `WriteUInt64Column`, `WriteUInt64ColumnFromBytes`

### Signed Integer Types
- **Int8**: `WriteInt8Column`, `WriteInt8ColumnFromBytes`
- **Int16**: `WriteInt16Column`, `WriteInt16ColumnFromBytes`
- **Int32**: `WriteInt32Column`, `WriteInt32ColumnFromBytes`
- **Int64**: `WriteInt64Column`, `WriteInt64ColumnFromBytes`

### Floating Point Types
- **Float32**: `WriteFloat32Column`, `WriteFloat32ColumnFromBytes`
- **Float64**: `WriteFloat64Column`, `WriteFloat64ColumnFromBytes`

### Date/Time Types
- **Date**: `WriteDateColumn`
- **DateTime**: `WriteDateTimeColumn`

### Other Types
- **Boolean**: `WriteBoolColumn`
- **UUID**: `WriteUUIDColumn`
- **IPv4/IPv6**: `WriteIPv4Column`, `WriteIPv6Column`
- **Nullable**: `WriteNullableStringColumn`
- **Array**: `WriteArrayStringColumn`
- **Tuple**: `WriteTupleColumn`

## Key Benefits

1. **Performance**: Direct byte-to-column writing with minimal overhead
2. **Type Safety**: Compile-time type checking for data types
3. **Memory Efficiency**: Proper column allocation and management
4. **Flexibility**: Support for all major ClickHouse data types
5. **Unsafe Pointer Support**: Efficient conversion from byte arrays to native types