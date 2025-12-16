// counter_processor_plugin.go 是计数器处理器插件的实现
package plugin

import "fmt"

// CounterProcessorPlugin 是计数器处理器插件
type CounterProcessorPlugin struct {
	*BaseProcessor
	id     string
	config ProcessorPluginConfig
	count  int64
}

// NewCounterProcessorPlugin 创建计数器处理器插件
func NewCounterProcessorPlugin(config ProcessorPluginConfig) ProcessorPlugin {
	return &CounterProcessorPlugin{
		id:     config.ID,
		config: config,
		count:  0,
	}
}

// ID 返回插件ID
func (cpp *CounterProcessorPlugin) ID() string {
	return cpp.id
}

// Type 返回插件类型
func (cpp *CounterProcessorPlugin) Type() PluginType {
	return ProcessorPluginType
}

// SetID 设置插件ID
func (cpp *CounterProcessorPlugin) SetID(id string) {
	cpp.id = id
}

// Init 初始化插件
func (cpp *CounterProcessorPlugin) Init(config map[string]interface{}) error {
	// 在实际实现中，这里可能需要处理配置
	return nil
}

// Close 关闭插件
func (cpp *CounterProcessorPlugin) Close() error {
	// 在实际实现中，这里可能需要释放资源
	return nil
}

// Process 实现计数处理逻辑
func (cpp *CounterProcessorPlugin) Process(ctx *ProcessorContext) (*ProcessorResult, error) {
	// 增加计数器
	cpp.count++
	
	// 在数据前添加计数信息
	processedData := []byte(fmt.Sprintf("[COUNT:%d] %s", cpp.count, string(ctx.Data)))
	
	// 获取目标处理器ID
	targetIDs := cpp.config.TargetProcessorIDs
	if len(targetIDs) == 0 {
		targetIDs = []string{} // 或者指定默认的目标处理器ID
	}
	
	result := &ProcessorResult{
		ProcessedData:      processedData,
		ShouldPersist:      true,
		TargetProcessorIDs: targetIDs,
	}
	
	return result, nil
}

// 自动注册计数器处理器插件
func init() {
	registry := GetGlobalRegistry()
	registry.RegisterProcessorPlugin("counter", func() ProcessorPlugin {
		return NewCounterProcessorPlugin(ProcessorPluginConfig{Type: "counter"})
	})
}