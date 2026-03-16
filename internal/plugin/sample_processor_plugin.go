// sample_processor_plugin.go 是示例处理器插件的实现
package plugin

import "berry-rapids-go/internal/model"

// SampleProcessorPlugin 是一个示例处理器插件
type SampleProcessorPlugin struct {
	*BaseProcessor
	id     string
	config ProcessorPluginConfig
}

// NewSampleProcessorPlugin 创建示例处理器插件
func NewSampleProcessorPlugin(config ProcessorPluginConfig) ProcessorPlugin {
	return &SampleProcessorPlugin{
		id:     config.ID,
		config: config,
	}
}

// ID 返回插件ID
func (spp *SampleProcessorPlugin) ID() string {
	return spp.id
}

// Type 返回插件类型
func (spp *SampleProcessorPlugin) Type() PluginType {
	return ProcessorPluginType
}

// SetID 设置插件ID
func (spp *SampleProcessorPlugin) SetID(id string) {
	spp.id = id
}

// Init 初始化插件
func (spp *SampleProcessorPlugin) Init(config map[string]interface{}) error {
	// 在实际实现中，这里可能需要处理配置
	return nil
}

// Close 关闭插件
func (spp *SampleProcessorPlugin) Close() error {
	// 在实际实现中，这里可能需要释放资源
	return nil
}

// Process 实现处理逻辑
func (spp *SampleProcessorPlugin) Process(ctx *ProcessorContext) (*ProcessorResult, error) {
	// 示例处理逻辑：将数据转换为大写并添加前缀
	dataStr := string(ctx.Data)
	processedData := []byte("SAMPLE_PROCESSED: " + dataStr)

	// 创建单行 FieldBatch
	batchData := make(map[string][][]byte)
	batchData["data"] = [][]byte{processedData}
	batch := model.NewFieldBatch(batchData)

	// 获取目标处理器ID
	targetIDs := spp.config.TargetProcessorIDs
	if len(targetIDs) == 0 {
		// 如果没有指定目标处理器，可以设置默认行为
		targetIDs = []string{} // 或者指定默认的目标处理器ID
	}

	result := &ProcessorResult{
		ProcessedData:      batch,
		ShouldPersist:      true, // 示例中设置为需要持久化
		TargetProcessorIDs: targetIDs,
	}

	return result, nil
}

// 自动注册示例处理器插件
func init() {
	registry := GetGlobalRegistry()
	registry.RegisterProcessorPlugin("sample", func() ProcessorPlugin {
		return NewSampleProcessorPlugin(ProcessorPluginConfig{Type: "sample"})
	})
}
