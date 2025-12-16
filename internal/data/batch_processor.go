// batch_processor.go 是Berry Rapids Go项目中批处理模块的核心实现文件。
// 该文件定义了BatchProcessor 结构体，实现了数据批处理和管道处理功能。
// 设计原理：通过批处理减少数据库写入次数，使用管道架构分离解析和持久化操作，提高系统性能
// 实现方式：使用互斥锁保证线程安全，通过通道实现生产者-消费者模式，支持定时刷新和容量刷新
package data

import (
	"context"
	"sync"
	"time"
	"berry-rapids-go/internal/model"
	"berry-rapids-go/internal/configuration"
	"go.uber.org/zap"
)

// BatchProcessor 处理数据批处理操作，在数据持久化之前进行批处理
// 设计说明：该结构体实现了生产者-消费者模式，支持定时刷新和容量刷新
type BatchProcessor struct {
	config          *configuration.BlockConfig // 批处理配置
	logger          *zap.Logger                // 日志记录器
	batch           *model.BatchData           // 当前批处理数据
	mutex           sync.Mutex                 // 互斥锁，保证线程安全
	flushCallback   func(*model.BatchData) error // 刷新回调函数
	flushTimer      *time.Timer                // 刷新定时器
	flushInterval   time.Duration              // 刷新间隔
	ctx             context.Context            // 上下文，用于控制goroutine生命周期
	
	// 管道字段，用于分离解析和持久化操作
	pipelineChan    chan *model.BatchData // 管道通道
	pipelineWorkers int                   // 管道工作线程数
	pipelineWG      sync.WaitGroup        // 等待组，用于等待所有工作线程完成
	pipelineClosed  bool                  // 管道是否已关闭
}

// NewBatchProcessor 创建新的批处理器实例
// 设计原理：初始化批处理器配置和管道架构，启动持久化工作线程
// 参数说明：
//   ctx - 上下文，用于控制goroutine生命周期
//   config - 批处理配置
//   logger - 日志记录器
//   flushInterval - 刷新间隔
//   flushCallback - 刷新回调函数
//   pipelineWorkers - 管道工作线程数
// 返回值：BatchProcessor实例
func NewBatchProcessor(
	ctx context.Context,
	config *configuration.BlockConfig,
	logger *zap.Logger,
	flushInterval time.Duration,
	flushCallback func(*model.BatchData) error,
	pipelineWorkers int,
) *BatchProcessor {
	// 创建批处理器实例
	bp := &BatchProcessor{
		config:          config,                              // 批处理配置
		logger:          logger,                              // 日志记录器
		flushCallback:   flushCallback,                       // 刷新回调函数
		flushInterval:   flushInterval,                       // 刷新间隔
		ctx:             ctx,                                 // 上下文
		pipelineChan:    make(chan *model.BatchData, config.GetByteBufferFixedCacheSize()), // 管道通道
		pipelineWorkers: pipelineWorkers,                     // 管道工作线程数
	}
	
	// 启动管道工作线程
	bp.startPipelineWorkers()
	
	// 返回批处理器实例
	return bp
}

// startPipelineWorkers 启动持久化工作线程
// 设计原理：为每个工作线程启动独立的goroutine，实现并行持久化
func (bp *BatchProcessor) startPipelineWorkers() {
	// 启动指定数量的持久化工作线程
	for i := 0; i < bp.pipelineWorkers; i++ {
		// 增加等待组计数
		bp.pipelineWG.Add(1)
		// 启动持久化工作线程
		go bp.persistenceWorker(i)
	}
}

// persistenceWorker 在独立的goroutine中处理持久化操作
// 设计原理：从管道通道中接收批数据并调用刷新回调函数进行持久化
// 参数说明：workerID - 工作线程ID
func (bp *BatchProcessor) persistenceWorker(workerID int) {
	// 在函数结束时减少等待组计数
	defer bp.pipelineWG.Done()
	
	// 记录工作线程启动日志
	bp.logger.Debug("Persistence worker started", zap.Int("workerID", workerID))
	
	// 从管道通道中接收批数据并处理，同时监听上下文取消信号
	for {
		select {
		case <-bp.ctx.Done():
			// 收到上下文取消信号，记录日志并退出循环
			bp.logger.Debug("Persistence worker stopping due to context cancellation", zap.Int("workerID", workerID))
			return
		case batch, ok := <-bp.pipelineChan:
			// 如果通道已关闭且没有更多数据，则退出循环
			if !ok {
				bp.logger.Debug("Pipeline channel closed, persistence worker stopping", zap.Int("workerID", workerID))
				return
			}
			
			// 记录处理批数据的日志
			bp.logger.Debug("Processing batch in persistence worker",
				zap.Int("workerID", workerID),
				zap.Int64("startRowID", batch.StartRowID),
				zap.Int64("endRowID", batch.EndRowID),
				zap.Int64("sizeInBytes", batch.SizeInBytes),
				zap.Int64("numberOfRows", batch.NumberOfRows))
			
			// 调用刷新回调函数进行持久化
			if err := bp.flushCallback(batch); err != nil {
				// 如果持久化失败，记录错误日志
				bp.logger.Error("Failed to flush batch in worker", 
					zap.Int("workerID", workerID),
					zap.Error(err))
			}
		}
	}
	
	// 记录工作线程停止日志
	bp.logger.Debug("Persistence worker stopped", zap.Int("workerID", workerID))
}

// AddData 将数据添加到当前批处理中
// 设计原理：将数据追加到批处理中，当批处理满时自动刷新
// 参数说明：data - 需要添加的数据，rowID - 行ID
// 返回值：添加过程中可能的错误
func (bp *BatchProcessor) AddData(data []byte, rowID int64) error {
	// 加锁保证线程安全
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	// 如果当前批处理为空，则初始化新的批处理
	if bp.batch == nil {
		bp.batch = model.NewBatchData(rowID, rowID, "default")
		// 重置刷新定时器
		bp.resetFlushTimer()
	}

	// 将数据追加到批处理中
	if bp.batch.Content == nil {
		bp.batch.Content = data
	} else {
		bp.batch.Content = append(bp.batch.Content, data...)
	}

	// 更新批处理的大小和行数信息
	bp.batch.SizeInBytes = int64(len(bp.batch.Content))
	bp.batch.NumberOfRows++
	bp.batch.EndRowID = rowID

	// 检查批处理是否已满，如果已满则刷新
	if bp.batch.IsFull(bp.config.GetBatchDataMaxRowCnt(), bp.config.GetBatchDataMaxByteSize()) {
		return bp.flush()
	}

	// 返回nil表示添加成功
	return nil
}

// Flush 强制处理当前批处理
// 设计原理：提供手动刷新批处理的接口
// 返回值：刷新过程中可能的错误
func (bp *BatchProcessor) Flush() error {
	// 加锁保证线程安全
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	// 调用内部刷新方法
	return bp.flush()
}

// flush 处理当前批处理（必须在持有锁的情况下调用）
// 设计原理：将批处理数据发送到管道通道进行异步持久化
// 返回值：刷新过程中可能的错误
func (bp *BatchProcessor) flush() error {
	// 如果当前批处理为空或没有数据，则直接返回
	if bp.batch == nil || bp.batch.IsEmpty() {
		return nil
	}

	// 记录刷新批处理的日志
	bp.logger.Debug("Flushing batch to pipeline",
		zap.Int64("startRowID", bp.batch.StartRowID),
		zap.Int64("endRowID", bp.batch.EndRowID),
		zap.Int64("sizeInBytes", bp.batch.SizeInBytes),
		zap.Int64("numberOfRows", bp.batch.NumberOfRows))

	// 将批处理数据发送到管道通道进行持久化
	// 使用select和default避免通道满时阻塞
	select {
	case bp.pipelineChan <- bp.batch:
		// 成功发送到管道通道，记录日志
		bp.logger.Debug("Batch sent to pipeline")
	default:
		// 管道通道已满，同步处理批处理数据
		bp.logger.Warn("Pipeline channel is full, processing batch synchronously")
		if err := bp.flushCallback(bp.batch); err != nil {
			// 如果同步处理失败，记录错误日志并返回错误
			bp.logger.Error("Failed to flush batch synchronously", zap.Error(err))
			return err
		}
	}

	// 重置批处理
	bp.batch = nil
	// 停止刷新定时器
	bp.stopFlushTimer()

	// 返回nil表示刷新成功
	return nil
}

// resetFlushTimer 重置刷新定时器
// 设计原理：重新设置定时器，在指定时间后触发刷新操作
func (bp *BatchProcessor) resetFlushTimer() {
	// 停止当前定时器
	bp.stopFlushTimer()
	// 创建新的定时器，在刷新间隔后触发刷新操作
	bp.flushTimer = time.AfterFunc(bp.flushInterval, func() {
		// 定时器触发时执行刷新操作
		if err := bp.Flush(); err != nil {
			// 如果刷新失败，记录错误日志
			bp.logger.Error("Failed to flush batch on timer", zap.Error(err))
		}
	})
}

// stopFlushTimer 停止刷新定时器
// 设计原理：停止当前的刷新定时器，避免重复刷新
func (bp *BatchProcessor) stopFlushTimer() {
	// 如果定时器存在，则停止定时器
	if bp.flushTimer != nil {
		bp.flushTimer.Stop()
	}
}

// Close 清理资源
// 设计原理：优雅关闭批处理器，确保所有数据都被处理
// 返回值：关闭过程中可能的错误
func (bp *BatchProcessor) Close() error {
	// 加锁保证线程安全
	bp.mutex.Lock()
	// 检查管道是否已关闭
	if bp.pipelineClosed {
		bp.mutex.Unlock()
		return nil
	}
	// 标记管道已关闭
	bp.pipelineClosed = true
	
	// 获取剩余的批处理数据
	var remainingBatch *model.BatchData
	if bp.batch != nil && !bp.batch.IsEmpty() {
		remainingBatch = bp.batch
	}
	// 解锁
	bp.mutex.Unlock()
	
	// 处理剩余的批处理数据
	if remainingBatch != nil {
		// 尝试将剩余批处理数据发送到管道通道
		select {
		case bp.pipelineChan <- remainingBatch:
		default:
			// 如果管道通道已满，则同步处理剩余批处理数据
			if err := bp.flushCallback(remainingBatch); err != nil {
				// 如果同步处理失败，记录错误日志
				bp.logger.Error("Failed to flush remaining batch", zap.Error(err))
			}
		}
	}
	
	// 关闭管道通道
	close(bp.pipelineChan)
	
	// 等待所有工作线程完成
	bp.pipelineWG.Wait()
	
	// 再次加锁
	bp.mutex.Lock()
	// 停止刷新定时器
	bp.stopFlushTimer()
	// 解锁
	bp.mutex.Unlock()
	
	// 返回nil表示关闭成功
	return nil
}