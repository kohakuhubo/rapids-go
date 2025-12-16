// kafka_source_plugin.go 是Kafka数据源插件的实现
package plugin

import (
	"berry-rapids-go/internal/data/source/kafka"
)

// KafkaSourcePlugin 是Kafka数据源插件的包装器
type KafkaSourcePlugin struct {
	*kafka.KafkaSource
	id     string
	config SourcePluginConfig
}

// NewKafkaSourcePlugin 创建Kafka数据源插件实例
func NewKafkaSourcePlugin(config SourcePluginConfig) SourcePlugin {
	return &KafkaSourcePlugin{
		id:     config.ID,
		config: config,
	}
}

// ID 返回插件ID
func (ksp *KafkaSourcePlugin) ID() string {
	return ksp.id
}

// Type 返回插件类型
func (ksp *KafkaSourcePlugin) Type() PluginType {
	return SourcePluginType
}

// SetID 设置插件ID
func (ksp *KafkaSourcePlugin) SetID(id string) {
	ksp.id = id
}

// Init 初始化插件
func (ksp *KafkaSourcePlugin) Init(config map[string]interface{}) error {
	// 从配置中提取Kafka连接参数
	brokers := make([]string, 0)
	if b, ok := config["brokers"].([]interface{}); ok {
		for _, broker := range b {
			if brokerStr, ok := broker.(string); ok {
				brokers = append(brokers, brokerStr)
			}
		}
	}
	
	topic := ""
	if t, ok := config["topic"].(string); ok {
		topic = t
	}
	
	// 创建Kafka源实例
	kafkaSource, err := kafka.NewKafkaSource(nil, brokers, topic)
	if err != nil {
		return err
	}
	
	ksp.KafkaSource = kafkaSource
	return nil
}

// Next 获取下一条数据
func (ksp *KafkaSourcePlugin) Next() (SourceEntry, error) {
	return ksp.KafkaSource.Next()
}

// Close 关闭插件
func (ksp *KafkaSourcePlugin) Close() error {
	return ksp.KafkaSource.Close()
}

// 自动注册Kafka数据源插件
func init() {
	registry := GetGlobalRegistry()
	registry.RegisterSourcePlugin("kafka", func() SourcePlugin {
		return NewKafkaSourcePlugin(SourcePluginConfig{Type: "kafka"})
	})
}