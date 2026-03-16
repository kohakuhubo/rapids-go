// mapping_config.go 定义MessagePack到ClickHouse的映射配置结构
package plugin

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"
)

// FieldType 定义字段类型
type FieldType string

const (
	FieldTypeString   FieldType = "string"
	FieldTypeInt8     FieldType = "int8"
	FieldTypeInt16    FieldType = "int16"
	FieldTypeInt32    FieldType = "int32"
	FieldTypeInt64    FieldType = "int64"
	FieldTypeUInt8    FieldType = "uint8"
	FieldTypeUInt16   FieldType = "uint16"
	FieldTypeUInt32   FieldType = "uint32"
	FieldTypeUInt64   FieldType = "uint64"
	FieldTypeFloat32  FieldType = "float32"
	FieldTypeFloat64  FieldType = "float64"
	FieldTypeBool     FieldType = "bool"
	FieldTypeDateTime FieldType = "datetime"
	FieldTypeDate     FieldType = "date"
	FieldTypeUUID     FieldType = "uuid"
	FieldTypeIPv4     FieldType = "ipv4"
	FieldTypeIPv6     FieldType = "ipv6"
)

// FieldMapping 定义MessagePack字段到ClickHouse字段的映射
type FieldMapping struct {
	MsgPackFieldName string    `json:"msgpack_field_name"` // MessagePack字段名
	MsgPackFieldType FieldType `json:"msgpack_field_type"` // MessagePack字段类型
	CHColumnName     string    `json:"ch_column_name"`     // ClickHouse列名
	CHColumnType     FieldType `json:"ch_column_type"`     // ClickHouse列类型
}

// MappingConfig 定义一个完整的映射配置
type MappingConfig struct {
	ID          string         `json:"id"`          // 唯一标识符，与Kafka topic或NATS subject匹配
	TableName   string         `json:"table_name"`  // ClickHouse表名
	Fields      []FieldMapping `json:"fields"`      // 字段映射列表
	Description string         `json:"description"` // 映射描述
}

// MappingConfigs 管理多个映射配置
type MappingConfigs struct {
	configs map[string]*MappingConfig // key: ID, value: MappingConfig
	mutex   sync.RWMutex
}

// NewMappingConfigs 创建新的映射配置管理器
func NewMappingConfigs() *MappingConfigs {
	return &MappingConfigs{
		configs: make(map[string]*MappingConfig),
	}
}

// Add 添加映射配置
func (mc *MappingConfigs) Add(config *MappingConfig) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}
	if config.ID == "" {
		return fmt.Errorf("config ID cannot be empty")
	}

	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	mc.configs[config.ID] = config
	return nil
}

// Get 根据ID获取映射配置
func (mc *MappingConfigs) Get(id string) (*MappingConfig, bool) {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	config, exists := mc.configs[id]
	return config, exists
}

// GetAll 获取所有映射配置
func (mc *MappingConfigs) GetAll() []*MappingConfig {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	configs := make([]*MappingConfig, 0, len(mc.configs))
	for _, config := range mc.configs {
		configs = append(configs, config)
	}
	return configs
}

// Remove 移除映射配置
func (mc *MappingConfigs) Remove(id string) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	delete(mc.configs, id)
}

// LoadFromFile 从JSON文件加载映射配置
func (mc *MappingConfigs) LoadFromFile(filePath string) error {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read mapping config file: %w", err)
	}

	var configs struct {
		Mappings []MappingConfig `json:"mappings"`
	}

	if err := json.Unmarshal(data, &configs); err != nil {
		return fmt.Errorf("failed to parse mapping config file: %w", err)
	}

	// 验证并添加所有配置
	for i := range configs.Mappings {
		if err := mc.Add(&configs.Mappings[i]); err != nil {
			return fmt.Errorf("failed to add mapping at index %d: %w", i, err)
		}
	}

	return nil
}

// Validate 验证映射配置的有效性
func (m *MappingConfig) Validate() error {
	if m.ID == "" {
		return fmt.Errorf("mapping ID cannot be empty")
	}
	if m.TableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if len(m.Fields) == 0 {
		return fmt.Errorf("fields cannot be empty")
	}

	// 验证每个字段映射
	for i, field := range m.Fields {
		if field.MsgPackFieldName == "" {
			return fmt.Errorf("field at index %d: msgpack_field_name cannot be empty", i)
		}
		if field.MsgPackFieldType == "" {
			return fmt.Errorf("field at index %d: msgpack_field_type cannot be empty", i)
		}
		if field.CHColumnName == "" {
			return fmt.Errorf("field at index %d: ch_column_name cannot be empty", i)
		}
		if field.CHColumnType == "" {
			return fmt.Errorf("field at index %d: ch_column_type cannot be empty", i)
		}
	}

	return nil
}

// GetFieldMapping 根据MessagePack字段名获取字段映射
func (m *MappingConfig) GetFieldMapping(msgpackFieldName string) (*FieldMapping, bool) {
	for i := range m.Fields {
		if m.Fields[i].MsgPackFieldName == msgpackFieldName {
			return &m.Fields[i], true
		}
	}
	return nil, false
}

// GetColumnNames 获取所有ClickHouse列名
func (m *MappingConfig) GetColumnNames() []string {
	columns := make([]string, len(m.Fields))
	for i, field := range m.Fields {
		columns[i] = field.CHColumnName
	}
	return columns
}

// GetMsgPackFieldNames 获取所有MessagePack字段名
func (m *MappingConfig) GetMsgPackFieldNames() []string {
	fields := make([]string, len(m.Fields))
	for i, field := range m.Fields {
		fields[i] = field.MsgPackFieldName
	}
	return fields
}
