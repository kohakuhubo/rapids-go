// engine.go 实现数据流转引擎
package plugin

import (
	"context"
	"sync"
	"time"
	"berry-rapids-go/internal/model"
	"go.uber.org/zap"
)

// DataFlowEngine 管理数据在插件间的流转
type DataFlowEngine struct {
	pluginManager      *PluginManager
	asyncPersistence   BatchDataSubmitter
	logger             *zap.Logger
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	sourceProcessors   map[string][]string // sourceID -> processorIDs
	processorChains    map[string][]string // processorID -> nextProcessorIDs
	dataChannels       map[string]chan *ProcessorContext
	channelBufferSize  int
	workersPerProcessor int
}

// NewDataFlowEngine 创建数据流转引擎
func NewDataFlowEngine(
	pluginManager *PluginManager,
	asyncPersistence BatchDataSubmitter,
	logger *zap.Logger,
	channelBufferSize int,
	workersPerProcessor int,
) *DataFlowEngine {
	ctx, cancel := context.WithCancel(context.Background())
	
	engine := &DataFlowEngine{
		pluginManager:      pluginManager,
		asyncPersistence:   asyncPersistence,
		logger:             logger,
		ctx:                ctx,
		cancel:             cancel,
		sourceProcessors:   make(map[string][]string),
		processorChains:    make(map[string][]string),
		dataChannels:       make(map[string]chan *ProcessorContext),
		channelBufferSize:  channelBufferSize,
		workersPerProcessor: workersPerProcessor,
	}
	
	return engine
}

// ConfigureSourceProcessorMapping 配置数据源到处理器的映射关系
func (e *DataFlowEngine) ConfigureSourceProcessorMapping(sourceID string, processorIDs []string) {
	e.sourceProcessors[sourceID] = processorIDs
}

// ConfigureProcessorChain 配置处理器链
func (e *DataFlowEngine) ConfigureProcessorChain(processorID string, nextProcessorIDs []string) {
	e.processorChains[processorID] = nextProcessorIDs
}

// Start 启动数据流转引擎
func (e *DataFlowEngine) Start() error {
	// 初始化所有处理器的数据通道
	allProcessors := e.pluginManager.GetAllProcessorPlugins()
	for id := range allProcessors {
		e.dataChannels[id] = make(chan *ProcessorContext, e.channelBufferSize)
	}
	
	// 启动所有处理器的工作线程
	for id := range allProcessors {
		for i := 0; i < e.workersPerProcessor; i++ {
			e.wg.Add(1)
			go e.processorWorker(id, i)
		}
	}
	
	// 启动所有数据源的消费循环
	allSources := e.pluginManager.GetAllSourcePlugins()
	for id, source := range allSources {
		e.wg.Add(1)
		go e.sourceConsumer(id, source)
	}
	
	return nil
}

// sourceConsumer 消费数据源的数据
func (e *DataFlowEngine) sourceConsumer(sourceID string, source SourcePlugin) {
	defer e.wg.Done()
	
	e.logger.Info("Starting source consumer", zap.String("sourceID", sourceID))
	
	for {
		select {
		case <-e.ctx.Done():
			e.logger.Info("Source consumer stopping", zap.String("sourceID", sourceID))
			return
		default:
			// 从数据源获取数据
			entry, err := source.Next()
			if err != nil {
				e.logger.Error("Failed to get data from source", zap.String("sourceID", sourceID), zap.Error(err))
				// 短暂等待后重试
				time.Sleep(100 * time.Millisecond)
				continue
			}
			
			// 创建处理器上下文
			ctx := &ProcessorContext{
				SourceID: sourceID,
				Data:     entry.GetValue(),
				RowID:    entry.GetRowId(),
			}
			
			// 发送到关联的处理器
			if processorIDs, exists := e.sourceProcessors[sourceID]; exists {
				for _, processorID := range processorIDs {
					e.sendToProcessor(processorID, ctx)
				}
			}
		}
	}
}

// processorWorker 处理器工作线程
func (e *DataFlowEngine) processorWorker(processorID string, workerID int) {
	defer e.wg.Done()
	
	processor, exists := e.pluginManager.GetProcessorPlugin(processorID)
	if !exists {
		e.logger.Error("Processor not found", zap.String("processorID", processorID))
		return
	}
	
	e.logger.Debug("Processor worker started", zap.String("processorID", processorID), zap.Int("workerID", workerID))
	
	// 获取处理器的数据通道
	channel, exists := e.dataChannels[processorID]
	if !exists {
		e.logger.Error("Data channel not found for processor", zap.String("processorID", processorID))
		return
	}
	
	for {
		select {
		case <-e.ctx.Done():
			e.logger.Debug("Processor worker stopping", zap.String("processorID", processorID), zap.Int("workerID", workerID))
			return
		case ctx, ok := <-channel:
			if !ok {
				e.logger.Debug("Data channel closed for processor", zap.String("processorID", processorID), zap.Int("workerID", workerID))
				return
			}
			
			// 处理数据
			result, err := processor.Process(ctx)
			if err != nil {
				e.logger.Error("Processor failed to process data", 
					zap.String("processorID", processorID), 
					zap.Int64("rowID", ctx.RowID), 
					zap.Error(err))
				continue
			}
			
			// 如果需要持久化，提交到持久化处理器
			if result.ShouldPersist {
				// 创建批数据并提交
				batch := model.NewBatchData(ctx.RowID, ctx.RowID, processorID)
				batch.Content = result.ProcessedData
				batch.SizeInBytes = int64(len(result.ProcessedData))
				batch.NumberOfRows = 1
				
				// 异步提交，不等待结果
				if err := e.asyncPersistence.Submit(batch); err != nil {
					e.logger.Error("Failed to submit batch to persistence", zap.Error(err))
				}
			}
			
			// 将处理结果发送到下一个处理器
			for _, nextProcessorID := range result.TargetProcessorIDs {
				nextCtx := &ProcessorContext{
					SourceID:            ctx.SourceID,
					PreviousProcessorID: processorID,
					Data:                result.ProcessedData,
					RowID:               ctx.RowID,
				}
				e.sendToProcessor(nextProcessorID, nextCtx)
			}
		}
	}
}

// sendToProcessor 发送数据到指定处理器
func (e *DataFlowEngine) sendToProcessor(processorID string, ctx *ProcessorContext) {
	channel, exists := e.dataChannels[processorID]
	if !exists {
		e.logger.Warn("Data channel not found for processor", zap.String("processorID", processorID))
		return
	}
	
	// 非阻塞发送，避免阻塞上游处理
	select {
	case channel <- ctx:
	default:
		e.logger.Warn("Processor channel is full, dropping data", 
			zap.String("processorID", processorID), 
			zap.Int64("rowID", ctx.RowID))
	}
}

// Stop 停止数据流转引擎
func (e *DataFlowEngine) Stop() error {
	// 取消上下文，停止所有工作线程
	e.cancel()
	
	// 等待所有工作线程完成
	e.wg.Wait()
	
	// 关闭所有数据通道
	for _, channel := range e.dataChannels {
		close(channel)
	}
	
	return nil
}

// GetStats 获取引擎统计信息
func (e *DataFlowEngine) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})
	
	// 添加持久化统计信息
	if e.asyncPersistence != nil {
		persistenceStats := e.asyncPersistence.GetStats()
		stats["persistence"] = persistenceStats
	}
	
	// 添加通道统计信息
	channelStats := make(map[string]interface{})
	for id, channel := range e.dataChannels {
		channelStats[id] = map[string]interface{}{
			"length":    len(channel),
			"capacity":  cap(channel),
		}
	}
	stats["channels"] = channelStats
	
	return stats
}