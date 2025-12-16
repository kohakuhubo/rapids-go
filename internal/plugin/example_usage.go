// example_usage.go 演示如何使用插件化架构
package plugin

import (
	"fmt"
	"berry-rapids-go/internal/data/persistece"
	"berry-rapids-go/internal/clickhouse/client"
	"berry-rapids-go/internal/configuration"
	"go.uber.org/zap"
)

// ExampleUsage 演示插件化架构的使用方法
func ExampleUsage() {
	// 创建日志记录器
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	
	// 创建插件管理器
	pluginManager := NewPluginManager()
	defer pluginManager.Close()
	
	// 创建ClickHouse客户端和持久化处理器
	clickhouseClient := client.NewEnhancedClickHouseClient(nil, logger)
	
	// 为了示例目的，创建一个BlockConfig对象
	// 在实际使用中，通常是从配置文件加载
	blockConfig := configuration.BlockConfig{}
	
	// 创建持久化处理器
	persistenceHandler := persistece.NewPersistenceHandler(
		logger,
		&blockConfig, // 使用默认配置
		clickhouseClient,
		"processed_data",
	)
	
	// 创建异步持久化处理器
	asyncPersistence := NewAsyncPersistenceHandler(
		persistenceHandler,
		logger,
		1000, // 队列大小
		4,    // 工作线程数
	)
	defer asyncPersistence.Close()
	
	// 注册数据源插件 - 示例中使用Kafka
	sourceConfig := SourcePluginConfig{
		ID:   "kafka-source-1",
		Type: "kafka",
		Config: map[string]interface{}{
			"brokers": []interface{}{"localhost:9092"},
			"topic":   "berry-rapids-topic",
		},
	}
	
	kafkaSource := NewKafkaSourcePlugin(sourceConfig)
	if err := kafkaSource.Init(sourceConfig.Config); err != nil {
		logger.Error("Failed to initialize Kafka source plugin", zap.Error(err))
		return
	}
	
	err := pluginManager.RegisterPlugin(kafkaSource)
	if err != nil {
		logger.Error("Failed to register Kafka source plugin", zap.Error(err))
		return
	}
	
	// 创建并注册处理器插件
	sampleProcessorConfig := ProcessorPluginConfig{
		ID:                  "sample-processor-1",
		Type:                "sample",
		SourceID:            "kafka-source-1",
		TargetProcessorIDs:  []string{"filter-processor-1"},
		Config:              map[string]interface{}{"param1": "value1"},
	}
	
	sampleProcessor := NewSampleProcessorPlugin(sampleProcessorConfig)
	err = pluginManager.RegisterPlugin(sampleProcessor)
	if err != nil {
		logger.Error("Failed to register sample processor plugin", zap.Error(err))
		return
	}
	
	// 创建过滤处理器
	filterProcessorConfig := ProcessorPluginConfig{
		ID:                  "filter-processor-1",
		Type:                "filter",
		PreviousProcessorID: "sample-processor-1",
		TargetProcessorIDs:  []string{"persistence-processor-1"},
		Config:              map[string]interface{}{"filterKeyword": "important"},
	}
	
	filterProcessor := NewFilterProcessorPlugin(filterProcessorConfig)
	err = pluginManager.RegisterPlugin(filterProcessor)
	if err != nil {
		logger.Error("Failed to register filter processor plugin", zap.Error(err))
		return
	}
	
	// 创建持久化处理器
	persistencePlugin := NewPersistencePlugin("persistence-processor-1", asyncPersistence)
	err = pluginManager.RegisterPlugin(persistencePlugin)
	if err != nil {
		logger.Error("Failed to register persistence processor plugin", zap.Error(err))
		return
	}
	
	fmt.Println("Example usage completed")
}