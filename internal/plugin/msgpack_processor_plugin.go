// msgpack_processor_plugin.go MessagePack处理器插件，整合映射配置、字段提取和ClickHouse输入构建
package plugin

import (
	"berry-rapids-go/internal/model"
	"fmt"
)

// MsgPackProcessorPlugin MessagePack处理器插件
type MsgPackProcessorPlugin struct {
	*BaseProcessor
	id          string
	config      ProcessorPluginConfig
	mappingRepo *MappingConfigs // 映射配置仓库
}

// NewMsgPackProcessorPlugin 创建MessagePack处理器插件
func NewMsgPackProcessorPlugin(config ProcessorPluginConfig, mappingRepo *MappingConfigs) ProcessorPlugin {
	return &MsgPackProcessorPlugin{
		id:          config.ID,
		config:      config,
		mappingRepo: mappingRepo,
	}
}

// ID 返回插件ID
func (mpp *MsgPackProcessorPlugin) ID() string {
	return mpp.id
}

// Type 返回插件类型
func (mpp *MsgPackProcessorPlugin) Type() PluginType {
	return ProcessorPluginType
}

// SetID 设置插件ID
func (mpp *MsgPackProcessorPlugin) SetID(id string) {
	mpp.id = id
}

// Init 初始化插件
func (mpp *MsgPackProcessorPlugin) Init(config map[string]interface{}) error {
	// 从配置中获取映射配置文件路径
	if mappingConfigPath, ok := config["mapping_config_path"].(string); ok {
		if err := mpp.mappingRepo.LoadFromFile(mappingConfigPath); err != nil {
			return fmt.Errorf("failed to load mapping config from %s: %w", mappingConfigPath, err)
		}
	}

	// 验证映射配置
	for _, mapping := range mpp.mappingRepo.GetAll() {
		if err := mapping.Validate(); err != nil {
			return fmt.Errorf("invalid mapping config for ID %s: %w", mapping.ID, err)
		}
	}

	return nil
}

// Close 关闭插件
func (mpp *MsgPackProcessorPlugin) Close() error {
	// 清理资源
	return nil
}

// Process 处理MessagePack数据并转换为ClickHouse Input
func (mpp *MsgPackProcessorPlugin) Process(ctx *ProcessorContext) (*ProcessorResult, error) {
	// 根据SourceID获取对应的映射配置
	// SourceID可以是Kafka topic或NATS subject
	mapping, exists := mpp.mappingRepo.Get(ctx.SourceID)
	if !exists {
		return nil, fmt.Errorf("no mapping config found for source ID: %s", ctx.SourceID)
	}

	// 自动检测数据类型：单个记录还是批量记录
	// 检查MessagePack数据的第一个字节
	if len(ctx.Data) == 0 {
		return nil, fmt.Errorf("empty MessagePack data")
	}

	typeCode := ctx.Data[0]

	// 判断是否为数组类型
	isArray := (typeCode >= 0x90 && typeCode <= 0x9F) || // fixarray
		typeCode == 0xDC || // array16
		typeCode == 0xDD // array32

	var fieldBatch *model.FieldBatch

	if isArray {
		// 批量数据处理
		batchData, err := ExtractAllFieldsBatch(ctx.Data, mapping)
		if err != nil {
			return nil, fmt.Errorf("failed to extract batch fields from MessagePack data: %w", err)
		}
		fieldBatch = batchData
	} else {
		// 单个记录处理
		singleData, err := ExtractAllFields(ctx.Data, mapping)
		if err != nil {
			return nil, fmt.Errorf("failed to extract fields from MessagePack data: %w", err)
		}
		fieldBatch = singleData
	}

	// 获取目标处理器ID
	targetIDs := mpp.config.TargetProcessorIDs
	if len(targetIDs) == 0 {
		// 如果没有指定目标处理器，可以设置默认行为
		targetIDs = []string{} // 或者指定默认的目标处理器ID
	}

	result := &ProcessorResult{
		ProcessedData:      fieldBatch,
		ShouldPersist:      true, // MessagePack处理后的数据需要持久化
		TargetProcessorIDs: targetIDs,
	}

	return result, nil
}

// GetMappingConfigs 获取所有映射配置
func (mpp *MsgPackProcessorPlugin) GetMappingConfigs() []*MappingConfig {
	return mpp.mappingRepo.GetAll()
}

// GetMappingConfig 获取指定ID的映射配置
func (mpp *MsgPackProcessorPlugin) GetMappingConfig(id string) (*MappingConfig, bool) {
	return mpp.mappingRepo.Get(id)
}

// AddMappingConfig 添加映射配置
func (mpp *MsgPackProcessorPlugin) AddMappingConfig(config *MappingConfig) error {
	return mpp.mappingRepo.Add(config)
}

// RemoveMappingConfig 移除映射配置
func (mpp *MsgPackProcessorPlugin) RemoveMappingConfig(id string) {
	mpp.mappingRepo.Remove(id)
}

// 自动注册MessagePack处理器插件
func init() {
	registry := GetGlobalRegistry()
	registry.RegisterProcessorPlugin("msgpack", func() ProcessorPlugin {
		return NewMsgPackProcessorPlugin(ProcessorPluginConfig{Type: "msgpack"}, NewMappingConfigs())
	})
}
