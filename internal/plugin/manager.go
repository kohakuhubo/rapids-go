// manager.go 实现了插件管理器，支持动态加载和注册插件
package plugin

import (
	"fmt"
	"sync"
)

// PluginManager 管理所有插件的注册和生命周期
type PluginManager struct {
	// plugins 存储所有注册的插件
	plugins map[string]Plugin

	// sourcePlugins 存储所有数据源插件
	sourcePlugins map[string]SourcePlugin

	// processorPlugins 存储所有处理器插件
	processorPlugins map[string]ProcessorPlugin

	// config 插件配置
	config *PluginConfig

	// mutex 保护并发访问
	mutex sync.RWMutex
}

// NewPluginManager 创建新的插件管理器实例
func NewPluginManager() *PluginManager {
	return &PluginManager{
		plugins:          make(map[string]Plugin),
		sourcePlugins:    make(map[string]SourcePlugin),
		processorPlugins: make(map[string]ProcessorPlugin),
	}
}

// LoadConfig 加载插件配置
func (pm *PluginManager) LoadConfig(config *PluginConfig) {
	pm.config = config
}

// InitializePlugins 根据配置初始化插件
func (pm *PluginManager) InitializePlugins() error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	// 获取全局注册表
	registry := GetGlobalRegistry()

	// 初始化数据源插件
	for _, sourceConfig := range pm.config.Plugins.Sources {
		// 从注册表创建插件实例
		sourcePlugin, err := registry.CreateSourcePlugin(sourceConfig.Type)
		if err != nil {
			return fmt.Errorf("failed to create source plugin %s: %w", sourceConfig.Type, err)
		}

		// 设置插件ID
		// 注意：这里需要根据具体插件实现来设置ID，可能需要通过接口或反射来实现
		// 为简化起见，我们假设插件有一个SetID方法
		if idSetter, ok := sourcePlugin.(interface{ SetID(string) }); ok {
			idSetter.SetID(sourceConfig.ID)
		}

		// 初始化插件
		if err := sourcePlugin.Init(sourceConfig.Config); err != nil {
			return fmt.Errorf("failed to initialize source plugin %s: %w", sourceConfig.ID, err)
		}

		// 注册插件
		pm.plugins[sourceConfig.ID] = sourcePlugin
		pm.sourcePlugins[sourceConfig.ID] = sourcePlugin
	}

	// 初始化处理器插件
	for _, processorConfig := range pm.config.Plugins.Processors {
		// 从注册表创建插件实例
		processorPlugin, err := registry.CreateProcessorPlugin(processorConfig.Type)
		if err != nil {
			return fmt.Errorf("failed to create processor plugin %s: %w", processorConfig.Type, err)
		}

		// 设置插件ID
		// 注意：这里需要根据具体插件实现来设置ID，可能需要通过接口或反射来实现
		// 为简化起见，我们假设插件有一个SetID方法
		if idSetter, ok := processorPlugin.(interface{ SetID(string) }); ok {
			idSetter.SetID(processorConfig.ID)
		}

		// 初始化插件
		if err := processorPlugin.Init(processorConfig.Config); err != nil {
			return fmt.Errorf("failed to initialize processor plugin %s: %w", processorConfig.ID, err)
		}

		// 注册插件
		pm.plugins[processorConfig.ID] = processorPlugin
		pm.processorPlugins[processorConfig.ID] = processorPlugin
	}

	return nil
}

// RegisterPlugin 注册插件
func (pm *PluginManager) RegisterPlugin(plugin Plugin) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	// 检查插件ID是否已存在
	if _, exists := pm.plugins[plugin.ID()]; exists {
		return fmt.Errorf("plugin with ID %s already registered", plugin.ID())
	}

	// 注册插件
	pm.plugins[plugin.ID()] = plugin

	// 根据插件类型分别注册到对应的映射中
	switch p := plugin.(type) {
	case SourcePlugin:
		pm.sourcePlugins[plugin.ID()] = p
	case ProcessorPlugin:
		pm.processorPlugins[plugin.ID()] = p
	}

	return nil
}

// GetPlugin 获取指定ID的插件
func (pm *PluginManager) GetPlugin(id string) (Plugin, bool) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	plugin, exists := pm.plugins[id]
	return plugin, exists
}

// GetSourcePlugin 获取指定ID的数据源插件
func (pm *PluginManager) GetSourcePlugin(id string) (SourcePlugin, bool) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	plugin, exists := pm.sourcePlugins[id]
	return plugin, exists
}

// GetProcessorPlugin 获取指定ID的处理器插件
func (pm *PluginManager) GetProcessorPlugin(id string) (ProcessorPlugin, bool) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	plugin, exists := pm.processorPlugins[id]
	return plugin, exists
}

// GetAllSourcePlugins 获取所有数据源插件
func (pm *PluginManager) GetAllSourcePlugins() map[string]SourcePlugin {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	// 创建副本以避免外部修改
	result := make(map[string]SourcePlugin)
	for id, plugin := range pm.sourcePlugins {
		result[id] = plugin
	}

	return result
}

// GetAllProcessorPlugins 获取所有处理器插件
func (pm *PluginManager) GetAllProcessorPlugins() map[string]ProcessorPlugin {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	// 创建副本以避免外部修改
	result := make(map[string]ProcessorPlugin)
	for id, plugin := range pm.processorPlugins {
		result[id] = plugin
	}

	return result
}

// UnregisterPlugin 注销插件
func (pm *PluginManager) UnregisterPlugin(id string) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	// 检查插件是否存在
	plugin, exists := pm.plugins[id]
	if !exists {
		return fmt.Errorf("plugin with ID %s not found", id)
	}

	// 关闭插件
	if err := plugin.Close(); err != nil {
		return fmt.Errorf("failed to close plugin %s: %w", id, err)
	}

	// 从所有映射中移除插件
	delete(pm.plugins, id)
	delete(pm.sourcePlugins, id)
	delete(pm.processorPlugins, id)

	return nil
}

// Close 关闭所有插件并释放资源
func (pm *PluginManager) Close() error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	// 关闭所有插件
	for id, plugin := range pm.plugins {
		if err := plugin.Close(); err != nil {
			return fmt.Errorf("failed to close plugin %s: %w", id, err)
		}
	}

	// 清空所有映射
	pm.plugins = make(map[string]Plugin)
	pm.sourcePlugins = make(map[string]SourcePlugin)
	pm.processorPlugins = make(map[string]ProcessorPlugin)

	return nil
}
