// nats_source_plugin.go 是NATS数据源插件的实现
package plugin

import (
	"berry-rapids-go/internal/data/source/nats"
)

// NatsSourcePlugin 是NATS数据源插件的包装器
type NatsSourcePlugin struct {
	*nats.NatsSource
	id     string
	config SourcePluginConfig
}

// NewNatsSourcePlugin 创建NATS数据源插件实例
func NewNatsSourcePlugin(config SourcePluginConfig) SourcePlugin {
	return &NatsSourcePlugin{
		id:     config.ID,
		config: config,
	}
}

// ID 返回插件ID
func (nsp *NatsSourcePlugin) ID() string {
	return nsp.id
}

// Type 返回插件类型
func (nsp *NatsSourcePlugin) Type() PluginType {
	return SourcePluginType
}

// SetID 设置插件ID
func (nsp *NatsSourcePlugin) SetID(id string) {
	nsp.id = id
}

// Init 初始化插件
func (nsp *NatsSourcePlugin) Init(config map[string]interface{}) error {
	// 从配置中提取NATS连接参数
	url := ""
	if u, ok := config["url"].(string); ok {
		url = u
	}
	
	subject := ""
	if s, ok := config["subject"].(string); ok {
		subject = s
	}
	
	stream := ""
	if s, ok := config["stream"].(string); ok {
		stream = s
	}
	
	// 创建NATS源实例
	natsSource, err := nats.NewNatsSource(nil, url, subject, stream)
	if err != nil {
		return err
	}
	
	nsp.NatsSource = natsSource
	return nil
}

// Next 获取下一条数据
func (nsp *NatsSourcePlugin) Next() (SourceEntry, error) {
	return nsp.NatsSource.Next()
}

// Close 关闭插件
func (nsp *NatsSourcePlugin) Close() error {
	return nsp.NatsSource.Close()
}

// 自动注册NATS数据源插件
func init() {
	registry := GetGlobalRegistry()
	registry.RegisterSourcePlugin("nats", func() SourcePlugin {
		return NewNatsSourcePlugin(SourcePluginConfig{Type: "nats"})
	})
}