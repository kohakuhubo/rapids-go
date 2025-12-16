// config.go 定义插件化配置结构
package plugin

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"berry-rapids-go/internal/data/source"
)

// PluginConfig 定义插件配置
type PluginConfig struct {
	Plugins struct {
		Sources     []SourcePluginConfig     `json:"sources"`
		Processors  []ProcessorPluginConfig  `json:"processors"`
	} `json:"plugins"`
	ClickHouse struct {
		Host       string `json:"host"`
		Port       int    `json:"port"`
		Database   string `json:"database"`
		Username   string `json:"username"`
		Password   string `json:"password"`
		DialTimeout string `json:"dialTimeout"`
		Compress   bool   `json:"compress"`
	} `json:"clickhouse"`
	System struct {
		ParseThreadSize           int `json:"parseThreadSize"`
		DataInsertThreadSize      int `json:"dataInsertThreadSize"`
		DataInsertQueueLength     int `json:"dataInsertQueueLength"`
		PluginChannelBufferSize   int `json:"pluginChannelBufferSize"`
		WorkersPerProcessor       int `json:"workersPerProcessor"`
		PersistenceQueueSize      int `json:"persistenceQueueSize"`
		PersistenceWorkers        int `json:"persistenceWorkers"`
	} `json:"system"`
}

// SourcePluginConfig 定义数据源插件配置
type SourcePluginConfig struct {
	ID     string                 `json:"id"`
	Type   string                 `json:"type"`
	Config map[string]interface{} `json:"config"`
}

// ConvertToDataSourceConfig 将SourcePluginConfig转换为DataSourceConfig
func (spc *SourcePluginConfig) ConvertToDataSourceConfig() *source.DataSourceConfig {
	config := &source.DataSourceConfig{
		Type: source.DataSourceType(spc.Type),
	}
	
	switch spc.Type {
	case "kafka":
		brokers := make([]string, 0)
		if b, ok := spc.Config["brokers"].([]interface{}); ok {
			for _, broker := range b {
				if brokerStr, ok := broker.(string); ok {
					brokers = append(brokers, brokerStr)
				}
			}
		}
		
		topic := ""
		if t, ok := spc.Config["topic"].(string); ok {
			topic = t
		}
		
		config.Kafka = &source.KafkaConfig{
			Brokers: brokers,
			Topic:   topic,
		}
	case "nats":
		url := ""
		if u, ok := spc.Config["url"].(string); ok {
			url = u
		}
		
		subject := ""
		if s, ok := spc.Config["subject"].(string); ok {
			subject = s
		}
		
		stream := ""
		if s, ok := spc.Config["stream"].(string); ok {
			stream = s
		}
		
		config.Nats = &source.NatsConfig{
			URL:     url,
			Subject: subject,
			Stream:  stream,
		}
	}
	
	return config
}

// LoadConfig 从JSON文件加载配置
func LoadConfig(filePath string) (*PluginConfig, error) {
	// 读取文件内容
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// 解析JSON配置
	config := &PluginConfig{}
	if err := json.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return config, nil
}