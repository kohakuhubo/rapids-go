// plugin_factory.go 实现插件工厂，用于根据配置创建插件
package plugin

import (
	"fmt"
)

// PluginFactory 定义插件工厂接口
type PluginFactory interface {
	CreateSourcePlugin(id string, config *SourcePluginConfig) (SourcePlugin, error)
	CreateProcessorPlugin(config *ProcessorPluginConfig) (ProcessorPlugin, error)
}

// DefaultPluginFactory 默认插件工厂实现
type DefaultPluginFactory struct{}

// NewDefaultPluginFactory 创建默认插件工厂
func NewDefaultPluginFactory() *DefaultPluginFactory {
	return &DefaultPluginFactory{}
}

// CreateSourcePlugin 根据配置创建数据源插件
func (f *DefaultPluginFactory) CreateSourcePlugin(id string, config *SourcePluginConfig) (SourcePlugin, error) {
	// 使用插件注册表创建插件
	registry := GetGlobalRegistry()
	sourcePlugin, err := registry.CreateSourcePlugin(config.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to create source plugin of type %s: %w", config.Type, err)
	}
	
	// 设置插件ID
	if setIDPlugin, ok := sourcePlugin.(SetIDPlugin); ok {
		setIDPlugin.SetID(id)
	} else {
		// 如果插件不支持设置ID，通过其他方式设置
		// 这里假设插件已经在创建时通过配置设置了ID
	}
	
	// 初始化插件
	if err := sourcePlugin.Init(config.Config); err != nil {
		return nil, fmt.Errorf("failed to initialize source plugin %s: %w", id, err)
	}
	
	return sourcePlugin, nil
}

// CreateProcessorPlugin 根据配置创建处理器插件
func (f *DefaultPluginFactory) CreateProcessorPlugin(config *ProcessorPluginConfig) (ProcessorPlugin, error) {
	// 使用插件注册表创建插件
	registry := GetGlobalRegistry()
	processorPlugin, err := registry.CreateProcessorPlugin(config.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to create processor plugin of type %s: %w", config.Type, err)
	}
	
	// 设置插件ID
	if setIDPlugin, ok := processorPlugin.(SetIDPlugin); ok {
		setIDPlugin.SetID(config.ID)
	} else {
		// 如果插件不支持设置ID，通过其他方式设置
		// 这里假设插件已经在创建时通过配置设置了ID
	}
	
	// 初始化插件
	if err := processorPlugin.Init(config.Config); err != nil {
		return nil, fmt.Errorf("failed to initialize processor plugin %s: %w", config.ID, err)
	}
	
	return processorPlugin, nil
}

// RegisterPluginFromConfig 根据配置注册插件
func RegisterPluginFromConfig(factory PluginFactory, pluginManager *PluginManager,
	sourceConfigs []SourcePluginConfig,
	processorConfigs []ProcessorPluginConfig) error {

	// 注册数据源插件
	for _, sourceConfig := range sourceConfigs {
		plugin, err := factory.CreateSourcePlugin(sourceConfig.ID, &sourceConfig)
		if err != nil {
			return fmt.Errorf("failed to create source plugin %s: %w", sourceConfig.ID, err)
		}

		if err := pluginManager.RegisterPlugin(plugin); err != nil {
			return fmt.Errorf("failed to register source plugin %s: %w", sourceConfig.ID, err)
		}
	}

	// 注册处理器插件
	for _, processorConfig := range processorConfigs {
		// 特殊处理持久化处理器
		if processorConfig.Type == "persistence" {
			// 持久化处理器在引擎中创建，这里跳过
			continue
		}

		plugin, err := factory.CreateProcessorPlugin(&processorConfig)
		if err != nil {
			return fmt.Errorf("failed to create processor plugin %s: %w", processorConfig.ID, err)
		}

		if err := pluginManager.RegisterPlugin(plugin); err != nil {
			return fmt.Errorf("failed to register processor plugin %s: %w", processorConfig.ID, err)
		}
	}

	return nil
}
