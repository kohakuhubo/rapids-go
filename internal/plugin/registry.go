// registry.go 实现插件注册表
package plugin

import (
	"fmt"
	"sync"
)

// PluginRegistry 插件注册表，用于存储所有已注册的插件创建函数
type PluginRegistry struct {
	sourceCreators     map[string]func() SourcePlugin
	processorCreators  map[string]func() ProcessorPlugin
	mutex              sync.RWMutex
}

// 全局插件注册表实例
var globalRegistry *PluginRegistry
var registryOnce sync.Once

// GetGlobalRegistry 获取全局插件注册表实例
func GetGlobalRegistry() *PluginRegistry {
	registryOnce.Do(func() {
		globalRegistry = &PluginRegistry{
			sourceCreators:    make(map[string]func() SourcePlugin),
			processorCreators: make(map[string]func() ProcessorPlugin),
		}
	})
	return globalRegistry
}

// RegisterSourcePlugin 注册数据源插件创建函数
func (r *PluginRegistry) RegisterSourcePlugin(pluginType string, creator func() SourcePlugin) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	if _, exists := r.sourceCreators[pluginType]; exists {
		return fmt.Errorf("source plugin type %s already registered", pluginType)
	}
	
	r.sourceCreators[pluginType] = creator
	return nil
}

// RegisterProcessorPlugin 注册处理器插件创建函数
func (r *PluginRegistry) RegisterProcessorPlugin(pluginType string, creator func() ProcessorPlugin) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	
	if _, exists := r.processorCreators[pluginType]; exists {
		return fmt.Errorf("processor plugin type %s already registered", pluginType)
	}
	
	r.processorCreators[pluginType] = creator
	return nil
}

// CreateSourcePlugin 创建数据源插件实例
func (r *PluginRegistry) CreateSourcePlugin(pluginType string) (SourcePlugin, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	
	creator, exists := r.sourceCreators[pluginType]
	if !exists {
		return nil, fmt.Errorf("source plugin type %s not registered", pluginType)
	}
	
	return creator(), nil
}

// CreateProcessorPlugin 创建处理器插件实例
func (r *PluginRegistry) CreateProcessorPlugin(pluginType string) (ProcessorPlugin, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	
	creator, exists := r.processorCreators[pluginType]
	if !exists {
		return nil, fmt.Errorf("processor plugin type %s not registered", pluginType)
	}
	
	return creator(), nil
}

// GetAllSourcePluginTypes 获取所有已注册的数据源插件类型
func (r *PluginRegistry) GetAllSourcePluginTypes() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	
	types := make([]string, 0, len(r.sourceCreators))
	for pluginType := range r.sourceCreators {
		types = append(types, pluginType)
	}
	
	return types
}

// GetAllProcessorPluginTypes 获取所有已注册的处理器插件类型
func (r *PluginRegistry) GetAllProcessorPluginTypes() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	
	types := make([]string, 0, len(r.processorCreators))
	for pluginType := range r.processorCreators {
		types = append(types, pluginType)
	}
	
	return types
}

// 自动注册插件的初始化函数
func init() {
	// 初始化全局注册表
	// 这里可以预先注册一些内置插件（如果需要）
}