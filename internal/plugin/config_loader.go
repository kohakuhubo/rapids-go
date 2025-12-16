// config_loader.go 从JSON文件加载插件配置
package plugin

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// LoadPluginConfig 从JSON文件加载插件配置
func LoadPluginConfig(configPath string) (*PluginConfig, error) {
	// 读取文件内容
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read plugin config file: %w", err)
	}

	// 解析JSON配置
	config := &PluginConfig{}
	if err := json.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("failed to parse plugin config file: %w", err)
	}

	return config, nil
}