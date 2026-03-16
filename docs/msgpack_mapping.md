# MessagePack到ClickHouse映射功能

## 概述

这个功能提供了一种灵活的方式来定义MessagePack消息中的字段与ClickHouse表字段之间的映射关系，并自动从MessagePack数据中提取字段数据并构建ClickHouse的输入格式。

**支持两种数据模式：**
- **单记录模式**：MessagePack消息包含单个对象（map）
- **批量模式**：MessagePack消息包含对象数组（array），系统会自动检测并处理

## 核心组件

### 1. 映射配置 (MappingConfig)

定义MessagePack字段到ClickHouse字段的映射关系。

**JSON配置文件格式：**

```json
{
  "mappings": [
    {
      "id": "kafka_topic_user_events",
      "table_name": "user_events",
      "description": "用户事件数据映射配置",
      "fields": [
        {
          "msgpack_field_name": "user_id",
          "msgpack_field_type": "uint64",
          "ch_column_name": "user_id",
          "ch_column_type": "uint64"
        },
        {
          "msgpack_field_name": "event_type",
          "msgpack_field_type": "string",
          "ch_column_name": "event_type",
          "ch_column_type": "string"
        }
      ]
    }
  ]
}
```

**字段说明：**

- `id`: 唯一标识符，与Kafka topic或NATS subject匹配
- `table_name`: 目标ClickHouse表名
- `description`: 映射配置的描述
- `fields`: 字段映射列表
  - `msgpack_field_name`: MessagePack字段名
  - `msgpack_field_type`: MessagePack字段类型
  - `ch_column_name`: ClickHouse列名
  - `ch_column_type`: ClickHouse列类型

### 2. 支持的数据类型

**MessagePack和ClickHouse都支持以下类型：**

- `string`: 字符串
- `int8`: 8位有符号整数
- `int16`: 16位有符号整数
- `int32`: 32位有符号整数
- `int64`: 64位有符号整数
- `uint8`: 8位无符号整数
- `uint16`: 16位无符号整数
- `uint32`: 32位无符号整数
- `uint64`: 64位无符号整数
- `float32`: 32位浮点数
- `float64`: 64位浮点数
- `bool`: 布尔值
- `datetime`: 日期时间
- `date`: 日期
- `uuid`: UUID
- `ipv4`: IPv4地址
- `ipv6`: IPv6地址

### 3. 核心类和方法

#### MappingConfigs

管理多个映射配置的仓库。

```go
// 创建映射配置仓库
mappingRepo := plugin.NewMappingConfigs()

// 从JSON文件加载配置
err := mappingRepo.LoadFromFile("configs/mapping_config.json")

// 获取指定ID的映射配置
mapping, exists := mappingRepo.Get("kafka_topic_user_events")

// 获取所有映射配置
configs := mappingRepo.GetAll()

// 添加新的映射配置
err := mappingRepo.Add(newMapping)

// 移除映射配置
mappingRepo.Remove("kafka_topic_user_events")
```

#### FieldExtractor

从MessagePack数据中提取字段数据（零拷贝）。

```go
// 创建字段提取器
extractor := plugin.NewFieldExtractor(msgpackData)

// 提取单个字段
data, err := extractor.ExtractFieldData("user_id", plugin.FieldTypeUInt64)

// 提取所有字段（根据映射配置）
fieldData, err := plugin.ExtractAllFields(msgpackData, mapping)
```

#### InputBuilder

根据字段数据构建ClickHouse的proto.Input。

```go
// 创建输入构建器
builder := plugin.NewInputBuilder(mapping)

// 构建ClickHouse Input
input, err := builder.BuildInput(fieldData)

// 获取表名
tableName := builder.GetTableName()

// 获取列名
columnNames := builder.GetColumnNames()
```

#### MsgPackProcessorPlugin

整合所有功能的处理器插件。

```go
// 创建处理器插件
processorConfig := plugin.ProcessorPluginConfig{
    ID:         "msgpack_processor_1",
    Type:       "msgpack",
    SourceID:   "kafka_topic_user_events",
    Config: map[string]interface{}{
        "mapping_config_path": "configs/mapping_config.json",
    },
}

processor := plugin.NewMsgPackProcessorPlugin(processorConfig, mappingRepo)

// 初始化插件
err := processor.Init(processorConfig.Config)

// 处理数据
ctx := &plugin.ProcessorContext{
    SourceID: "kafka_topic_user_events",
    Data:     msgpackData,
    RowID:    1,
}

result, err := processor.Process(ctx)
```

## 使用示例

### 完整示例

参见 `examples/msgpack_mapping_example.go` 文件。

### 基本流程

1. **定义映射配置**
   - 创建JSON配置文件，定义字段映射关系
   - 映射ID与Kafka topic或NATS subject匹配

2. **加载映射配置**
   ```go
   mappingRepo := plugin.NewMappingConfigs()
   err := mappingRepo.LoadFromFile("configs/mapping_config.json")
   ```

3. **创建处理器插件**
   ```go
   processor := plugin.NewMsgPackProcessorPlugin(config, mappingRepo)
   err := processor.Init(config.Config)
   ```

4. **处理MessagePack数据**
   ```go
   ctx := &plugin.ProcessorContext{
       SourceID: "kafka_topic_user_events",
       Data:     msgpackData,
       RowID:    1,
   }
   result, err := processor.Process(ctx)
   ```

5. **获取结果**
   - `result.ProcessedData`: 处理后的数据
   - `result.ShouldPersist`: 是否需要持久化
   - `result.TargetProcessorIDs`: 目标处理器ID列表

## 集成到现有系统

### 在插件配置中使用

在 `app.yaml` 中配置MessagePack处理器插件：

```yaml
plugins:
  processors:
    - id: msgpack_processor_1
      type: msgpack
      source_id: kafka_topic_user_events
      config:
        mapping_config_path: configs/mapping_config.json
      target_processor_ids:
        - clickhouse_inserter_1
```

### 与Kafka和NATS集成

- 映射配置的 `id` 字段应该与Kafka topic或NATS subject匹配
- 处理器会根据 `SourceID` 自动查找对应的映射配置
- 每个Kafka topic或NATS subject可以有独立的映射配置

## 性能优化

### 零拷贝设计

- 字段提取器使用零拷贝技术，尽量减少内存分配
- 使用unsafe指针直接操作字节数组
- 避免不必要的数据复制

### 批量处理支持

系统支持两种数据模式，自动检测并处理：

**单记录模式：**
```json
{
  "user_id": 123456789,
  "event_type": "login",
  "event_time": 1640995200
}
```

**批量模式：**
```json
[
  {
    "user_id": 123456789,
    "event_type": "login",
    "event_time": 1640995200
  },
  {
    "user_id": 123456790,
    "event_type": "logout",
    "event_time": 1640995260
  },
  {
    "user_id": 123456791,
    "event_type": "click",
    "event_time": 1640995320
  }
]
```

**批量处理优势：**
- 减少网络传输次数
- 提高ClickHouse插入效率
- 降低系统开销
- 支持流式处理大量数据

**批量处理API：**

```go
// 批量提取字段数据
fieldDataBatch, err := plugin.ExtractAllFieldsBatch(msgpackBytes, mapping)
// 返回: map[string][][]byte - 每个字段对应一个切片，包含所有记录的数据

// 批量构建ClickHouse Input
builder := plugin.NewInputBuilder(mapping)
input, err := builder.BuildInputBatch(fieldDataBatch)
```

**自动检测机制：**
- 处理器会自动检测MessagePack数据的类型（单个对象或数组）
- 根据数据类型选择合适的处理方式
- 无需用户手动指定模式

### 批处理性能

- **单次遍历**：批量提取时只遍历一次数据
- **提前退出**：找到所有字段后可以提前退出
- **内存优化**：使用切片三参数语法防止数据复制
- **并行处理**：支持并行处理多个批量数据

## 错误处理

- 字段不存在时返回错误
- 类型不匹配时返回错误
- 数据长度不足时返回错误
- 映射配置无效时返回错误

## 扩展性

### 添加新的数据类型

1. 在 `FieldType` 枚举中添加新类型
2. 在 `FieldExtractor` 中添加类型验证逻辑
3. 在 `InputBuilder` 中添加列构建逻辑
4. 在 `ColumnWriter` 中添加写入方法（如果需要）

### 自定义字段转换

可以在 `InputBuilder` 中扩展 `buildColumnInput` 方法，添加自定义的字段转换逻辑。

## 注意事项

1. **类型匹配**: MessagePack字段类型和ClickHouse列类型应该兼容
2. **字段顺序**: 映射配置中的字段顺序会影响ClickHouse列的顺序
3. **内存管理**: 虽然使用零拷贝，但仍需注意大消息的内存使用
4. **错误处理**: 建议在生产环境中添加完善的错误处理和日志记录
5. **配置验证**: 使用前务必验证映射配置的有效性

## 相关文件

- `internal/plugin/mapping_config.go`: 映射配置定义
- `internal/plugin/msgpack_field_extractor.go`: 字段提取器
- `internal/plugin/clickhouse_input_builder.go`: ClickHouse输入构建器
- `internal/plugin/msgpack_processor_plugin.go`: MessagePack处理器插件
- `pkg/clickhouse/column_writer.go`: 列写入器
- `configs/mapping_config.json`: 示例配置文件
- `examples/msgpack_mapping_example.go`: 使用示例