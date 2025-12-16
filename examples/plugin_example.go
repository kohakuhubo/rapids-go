// plugin_test.go 是插件系统测试文件
package main

import (
	"fmt"
	"berry-rapids-go/internal/plugin"
)

func main() {
	// 创建插件管理器
	pluginManager := plugin.NewPluginManager()

	// 加载配置
	config, err := plugin.LoadConfig("./configs/app.json")
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		return
	}

	// 将配置加载到插件管理器
	pluginManager.LoadConfig(config)

	// 创建插件工厂
	factory := plugin.NewDefaultPluginFactory()

	// 注册插件
	if err := plugin.RegisterPluginFromConfig(factory, pluginManager, config.Plugins.Sources, config.Plugins.Processors); err != nil {
		fmt.Printf("Failed to register plugins: %v\n", err)
		return
	}

	// 获取所有已注册的数据源插件类型
	registry := plugin.GetGlobalRegistry()
	sourceTypes := registry.GetAllSourcePluginTypes()
	fmt.Printf("Registered source plugin types: %v\n", sourceTypes)

	// 获取所有已注册的处理器插件类型
	processorTypes := registry.GetAllProcessorPluginTypes()
	fmt.Printf("Registered processor plugin types: %v\n", processorTypes)

	// 获取所有数据源插件
	sources := pluginManager.GetAllSourcePlugins()
	fmt.Printf("Initialized source plugins: %v\n", len(sources))

	// 获取所有处理器插件
	processors := pluginManager.GetAllProcessorPlugins()
	fmt.Printf("Initialized processor plugins: %v\n", len(processors))

	fmt.Println("Plugin system test completed successfully!")
}