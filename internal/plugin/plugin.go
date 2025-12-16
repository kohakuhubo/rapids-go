// plugin.go 定义了插件化架构的核心接口
package plugin

// PluginType 定义插件类型
type PluginType string

const (
	SourcePluginType   PluginType = "source"
	ProcessorPluginType PluginType = "processor"
)

// Plugin 定义插件的基本接口
type Plugin interface {
	// ID 返回插件的唯一标识符
	ID() string
	
	// Type 返回插件类型
	Type() PluginType
	
	// Init 初始化插件
	Init(config map[string]interface{}) error
	
	// Close 关闭插件并释放资源
	Close() error
}

// SourceEntry 定义数据源条目接口
type SourceEntry interface {
	GetValue() []byte
	GetRowId() int64
}

// SourcePlugin 定义数据源插件接口
type SourcePlugin interface {
	Plugin
	
	// Next 获取下一条数据
	Next() (SourceEntry, error)
}

// ProcessorContext 定义处理器上下文
type ProcessorContext struct {
	// SourceID 数据来源插件ID
	SourceID string
	
	// PreviousProcessorID 上一个处理器插件ID
	PreviousProcessorID string
	
	// Data 当前处理的数据
	Data []byte
	
	// RowID 数据行ID
	RowID int64
}

// ProcessorResult 定义处理器结果
type ProcessorResult struct {
	// ProcessedData 处理后的数据
	ProcessedData []byte
	
	// ShouldPersist 是否需要持久化
	ShouldPersist bool
	
	// TargetProcessorIDs 目标处理器ID列表
	TargetProcessorIDs []string
}

// ProcessorPlugin 定义数据处理器插件接口
type ProcessorPlugin interface {
	Plugin
	
	// Process 处理数据
	Process(ctx *ProcessorContext) (*ProcessorResult, error)
}

// SetIDPlugin 支持设置ID的插件接口
type SetIDPlugin interface {
	Plugin
	SetID(id string)
}