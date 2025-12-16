package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	
	"berry-rapids-go/internal/app"
	"berry-rapids-go/internal/configuration"
	"berry-rapids-go/internal/plugin"
	"berry-rapids-go/internal/data/persistece"
	"berry-rapids-go/internal/clickhouse/client"
	"go.uber.org/zap"
)

func main() {
	fmt.Println("Starting Berry Rapids Go application...")
	
	// 检查是否使用插件机制
	usePlugins := os.Getenv("USE_PLUGINS") == "true"
	
	if usePlugins {
		// 使用插件机制
		runWithPlugins()
	} else {
		// 使用传统机制
		runTraditional()
	}
}

// runTraditional 使用传统的服务器启动方式
func runTraditional() {
	// Create and start the server
	server := app.NewServer()
	
	// Handle graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	
	// Run the server in a goroutine
	go func() {
		server.Start()
	}()
	
	// Wait for interrupt signal
	<-c
	
	// Shutdown the server
	server.Stop()
	
	fmt.Println("Server shutdown complete")
}

// runWithPlugins 使用插件机制启动应用
func runWithPlugins() {
	// 创建日志记录器
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	
	// 加载应用配置
	appConfig, err := configuration.LoadConfig()
	if err != nil {
		logger.Fatal("Failed to load application config", zap.Error(err))
	}
	
	// 加载插件配置
	pluginConfig, err := plugin.LoadPluginConfig("./configs/app.json")
	if err != nil {
		logger.Fatal("Failed to load plugin config", zap.Error(err))
	}
	
	// 创建插件管理器
	pluginManager := plugin.NewPluginManager()
	
	// 将配置加载到插件管理器
	pluginManager.LoadConfig(pluginConfig)
	
	// 创建插件工厂
	factory := plugin.NewDefaultPluginFactory()
	
	// 注册插件
	if err := plugin.RegisterPluginFromConfig(factory, pluginManager, pluginConfig.Plugins.Sources, pluginConfig.Plugins.Processors); err != nil {
		logger.Fatal("Failed to register plugins", zap.Error(err))
	}
	
	// 创建ClickHouse客户端和持久化处理器
	clickhouseClient := client.NewEnhancedClickHouseClient(nil, logger)
	
	// 创建持久化处理器
	persistenceHandler := persistece.NewPersistenceHandler(
		logger,
		&appConfig.Block, // 使用应用配置中的块配置
		clickhouseClient,
		"processed_data",
	)
	
	// 创建异步持久化处理器
	asyncPersistence := plugin.NewAsyncPersistenceHandler(
		persistenceHandler,
		logger,
		pluginConfig.System.PersistenceQueueSize, // 使用插件配置中的队列大小
		pluginConfig.System.PersistenceWorkers,   // 使用插件配置中的工作线程数
	)
	defer asyncPersistence.Close()
	
	// 创建数据流转引擎
	engine := plugin.NewDataFlowEngine(
		pluginManager,
		asyncPersistence,
		logger,
		pluginConfig.System.PluginChannelBufferSize, // 使用插件配置中的通道缓冲区大小
		pluginConfig.System.WorkersPerProcessor,     // 使用插件配置中的每个处理器的工作线程数
	)
	
	// 启动数据流转引擎
	if err := engine.Start(); err != nil {
		logger.Fatal("Failed to start data flow engine", zap.Error(err))
	}
	
	// Handle graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	
	// Wait for interrupt signal
	<-c
	
	// 停止引擎
	engine.Stop()
	
	fmt.Println("Plugin-based server shutdown complete")
}