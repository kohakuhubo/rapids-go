// source_adapter.go 实现了将现有Source转换为SourcePlugin的适配器
package plugin

import (
	"berry-rapids-go/internal/data/source"
)

// sourceAdapter 将现有Source实现适配为SourcePlugin
type sourceAdapter struct {
	id     string
	source source.Source
}

// ID 返回插件ID
func (sa *sourceAdapter) ID() string {
	return sa.id
}

// Type 返回插件类型
func (sa *sourceAdapter) Type() PluginType {
	return SourcePluginType
}

// SetID 设置插件ID
func (sa *sourceAdapter) SetID(id string) {
	sa.id = id
}

// Init 初始化插件
func (sa *sourceAdapter) Init(config map[string]interface{}) error {
	// 适配器不需要额外初始化
	return nil
}

// Close 关闭插件
func (sa *sourceAdapter) Close() error {
	return sa.source.Close()
}

// Next 获取下一条数据
func (sa *sourceAdapter) Next() (SourceEntry, error) {
	return sa.source.Next()
}