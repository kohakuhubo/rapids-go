// filter_processor_plugin.go 是过滤处理器插件的实现
package plugin

import (
	"berry-rapids-go/internal/model"
	"strings"
)

// FilterProcessorPlugin 是过滤处理器插件
type FilterProcessorPlugin struct {
	*BaseProcessor
	id            string
	config        ProcessorPluginConfig
	filterKeyword string
}

// NewFilterProcessorPlugin 创建过滤处理器插件
func NewFilterProcessorPlugin(config ProcessorPluginConfig) ProcessorPlugin {
	// 从配置中获取过滤关键字
	filterKeyword := ""
	if keyword, ok := config.Config["filterKeyword"].(string); ok {
		filterKeyword = keyword
	}

	return &FilterProcessorPlugin{
		id:            config.ID,
		config:        config,
		filterKeyword: filterKeyword,
	}
}

// ID 返回插件ID
func (fpp *FilterProcessorPlugin) ID() string {
	return fpp.id
}

// Type 返回插件类型
func (fpp *FilterProcessorPlugin) Type() PluginType {
	return ProcessorPluginType
}

// SetID 设置插件ID
func (fpp *FilterProcessorPlugin) SetID(id string) {
	fpp.id = id
}

// Init 初始化插件
func (fpp *FilterProcessorPlugin) Init(config map[string]interface{}) error {
	// 从配置中获取过滤关键字
	if keyword, ok := config["filterKeyword"].(string); ok {
		fpp.filterKeyword = keyword
	}
	return nil
}

// Close 关闭插件
func (fpp *FilterProcessorPlugin) Close() error {
	// 在实际实现中，这里可能需要释放资源
	return nil
}

// Process 实现过滤处理逻辑
func (fpp *FilterProcessorPlugin) Process(ctx *ProcessorContext) (*ProcessorResult, error) {
	dataStr := string(ctx.Data)

	// 如果数据包含过滤关键字，则处理；否则丢弃
	if fpp.filterKeyword == "" || strings.Contains(dataStr, fpp.filterKeyword) {
		// 添加过滤标记
		processedData := []byte("[FILTERED] " + dataStr)

		// 创建单行 FieldBatch
		batchData := make(map[string][][]byte)
		batchData["data"] = [][]byte{processedData}
		batch := model.NewFieldBatch(batchData)

		// 获取目标处理器ID
		targetIDs := fpp.config.TargetProcessorIDs
		if len(targetIDs) == 0 {
			targetIDs = []string{} // 或者指定默认的目标处理器ID
		}

		result := &ProcessorResult{
			ProcessedData:      batch,
			ShouldPersist:      true,
			TargetProcessorIDs: targetIDs,
		}

		return result, nil
	}

	// 如果数据不包含过滤关键字，返回空结果，不传递给其他处理器
	// 创建单行 FieldBatch，包含原始数据
	batchData := make(map[string][][]byte)
	batchData["data"] = [][]byte{ctx.Data}
	batch := model.NewFieldBatch(batchData)

	result := &ProcessorResult{
		ProcessedData:      batch,      // 保持原始数据
		ShouldPersist:      false,      // 不需要持久化
		TargetProcessorIDs: []string{}, // 不传递给其他处理器
	}
	return result, nil
}

// 自动注册过滤处理器插件
func init() {
	registry := GetGlobalRegistry()
	registry.RegisterProcessorPlugin("filter", func() ProcessorPlugin {
		return NewFilterProcessorPlugin(ProcessorPluginConfig{Type: "filter"})
	})
}
