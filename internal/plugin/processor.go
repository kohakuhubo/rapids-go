// processor.go 定义了处理器插件的基础实现
package plugin

// BaseProcessor 提供处理器插件的基础实现
type BaseProcessor struct {
	id string
	config map[string]interface{}
}

// NewBaseProcessor 创建基础处理器实例
func NewBaseProcessor(id string) *BaseProcessor {
	return &BaseProcessor{
		id: id,
		config: make(map[string]interface{}),
	}
}

// ID 返回插件ID
func (bp *BaseProcessor) ID() string {
	return bp.id
}

// SetID 设置插件ID
func (bp *BaseProcessor) SetID(id string) {
	bp.id = id
}

// Type 返回插件类型
func (bp *BaseProcessor) Type() PluginType {
	return ProcessorPluginType
}

// Init 初始化插件
func (bp *BaseProcessor) Init(config map[string]interface{}) error {
	bp.config = config
	return nil
}

// Close 关闭插件
func (bp *BaseProcessor) Close() error {
	// 基础处理器不需要特殊关闭操作
	return nil
}

// ProcessorPluginConfig 定义处理器插件配置
type ProcessorPluginConfig struct {
	ID                  string   `json:"id"`
	Type                string   `json:"type"`
	SourceID            string   `json:"source_id,omitempty"`             // 数据源ID
	PreviousProcessorID string   `json:"previous_processor_id,omitempty"` // 上一个处理器ID
	TargetProcessorIDs  []string `json:"target_processor_ids,omitempty"`  // 目标处理器ID列表
	Config              map[string]interface{} `json:"config,omitempty"`  // 处理器特定配置
}