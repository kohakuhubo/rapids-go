// async_persistence.go 实现异步持久化机制
package plugin

import (
	"berry-rapids-go/internal/data/persistece"
	"berry-rapids-go/internal/model"
	"context"
	"go.uber.org/zap"
	"sync"
)

// AsyncPersistenceHandler 处理异步持久化操作
type AsyncPersistenceHandler struct {
	persistenceHandler *persistece.PersistenceHandler
	queue              chan *model.BatchData
	workers            int
	ctx                context.Context
	cancel             context.CancelFunc
	logger             *zap.Logger
	wg                 sync.WaitGroup
	closed             bool
	mutex              sync.RWMutex
}

// NewAsyncPersistenceHandler 创建异步持久化处理器
func NewAsyncPersistenceHandler(
	persistenceHandler *persistece.PersistenceHandler,
	logger *zap.Logger,
	queueSize int,
	workers int,
) *AsyncPersistenceHandler {
	ctx, cancel := context.WithCancel(context.Background())

	handler := &AsyncPersistenceHandler{
		persistenceHandler: persistenceHandler,
		queue:              make(chan *model.BatchData, queueSize),
		workers:            workers,
		ctx:                ctx,
		cancel:             cancel,
		logger:             logger,
		closed:             false,
	}

	// 启动工作线程
	handler.startWorkers()

	return handler
}

// startWorkers 启动工作线程处理持久化任务
func (aph *AsyncPersistenceHandler) startWorkers() {
	for i := 0; i < aph.workers; i++ {
		aph.wg.Add(1)
		go aph.worker(i)
	}
}

// worker 工作线程函数
func (aph *AsyncPersistenceHandler) worker(workerID int) {
	defer aph.wg.Done()

	aph.logger.Debug("Persistence worker started", zap.Int("workerID", workerID))

	for {
		select {
		case <-aph.ctx.Done():
			// 上下文取消，退出工作线程
			aph.logger.Debug("Persistence worker stopping due to context cancellation", zap.Int("workerID", workerID))
			return
		case batch, ok := <-aph.queue:
			if !ok {
				// 队列已关闭，退出工作线程
				aph.logger.Debug("Persistence worker stopping due to queue closure", zap.Int("workerID", workerID))
				return
			}

			// 处理持久化
			if err := aph.persistenceHandler.Handle(aph.ctx, batch); err != nil {
				aph.logger.Error("Failed to persist batch", zap.Error(err), zap.Int64("startRowID", batch.StartRowID))
			} else {
				aph.logger.Debug("Successfully persisted batch", zap.Int64("startRowID", batch.StartRowID))
			}
		}
	}
}

// Submit 提交批数据进行异步持久化
func (aph *AsyncPersistenceHandler) Submit(batch *model.BatchData) error {
	aph.mutex.RLock()
	defer aph.mutex.RUnlock()

	if aph.closed {
		return nil // 如果已关闭，直接返回，不处理数据
	}

	// 使用非阻塞方式发送到队列
	select {
	case aph.queue <- batch:
		return nil
	default:
		// 队列已满，记录警告但不阻塞
		aph.logger.Warn("Persistence queue is full, dropping batch", zap.Int64("startRowID", batch.StartRowID))
		return nil // 不返回错误，避免影响上游处理
	}
}

// Close 关闭异步持久化处理器
func (aph *AsyncPersistenceHandler) Close() error {
	aph.mutex.Lock()
	if aph.closed {
		aph.mutex.Unlock()
		return nil
	}
	aph.closed = true
	aph.mutex.Unlock()

	// 取消上下文，停止所有工作线程
	aph.cancel()

	// 关闭队列
	close(aph.queue)

	// 等待所有工作线程完成
	aph.wg.Wait()

	return nil
}

// GetStats 获取统计信息
func (aph *AsyncPersistenceHandler) GetStats() map[string]interface{} {
	aph.mutex.RLock()
	defer aph.mutex.RUnlock()

	stats := aph.persistenceHandler.GetStats()
	stats["queueLength"] = len(aph.queue)
	stats["queueCapacity"] = cap(aph.queue)
	stats["workers"] = aph.workers
	stats["closed"] = aph.closed

	return stats
}

// BatchDataSubmitter 定义批数据提交接口
type BatchDataSubmitter interface {
	Submit(batch *model.BatchData) error
	Close() error
	GetStats() map[string]interface{}
}

// PersistencePlugin 是一个特殊的处理器插件，用于提交数据到持久化层
type PersistencePlugin struct {
	*BaseProcessor
	asyncHandler BatchDataSubmitter
}

// NewPersistencePlugin 创建持久化插件
func NewPersistencePlugin(id string, asyncHandler BatchDataSubmitter) *PersistencePlugin {
	base := NewBaseProcessor(id)

	return &PersistencePlugin{
		BaseProcessor: base,
		asyncHandler:  asyncHandler,
	}
}

// Process 处理数据并提交到持久化层
func (pp *PersistencePlugin) Process(ctx *ProcessorContext) (*ProcessorResult, error) {
	// 创建批数据并提交到异步持久化处理器
	batch := model.NewBatchData(ctx.RowID, ctx.RowID, "plugin")
	// 创建 FieldBatch 对象
	batchData := make(map[string][][]byte)
	batchData["data"] = [][]byte{ctx.Data}
	batch.Content = model.NewFieldBatch(batchData)
	batch.SizeInBytes = int64(len(ctx.Data))
	batch.NumberOfRows = 1

	// 异步提交，不等待结果
	if err := pp.asyncHandler.Submit(batch); err != nil {
		return nil, err
	}

	// 返回结果，不传递给其他处理器
	// 创建 FieldBatch 对象用于返回
	resultBatchData := make(map[string][][]byte)
	resultBatchData["data"] = [][]byte{ctx.Data}
	resultBatch := model.NewFieldBatch(resultBatchData)

	return &ProcessorResult{
		ProcessedData:      resultBatch,
		ShouldPersist:      false,      // 已经提交到持久化层
		TargetProcessorIDs: []string{}, // 不传递给其他处理器
	}, nil
}

// Close 关闭持久化插件
func (pp *PersistencePlugin) Close() error {
	return pp.asyncHandler.Close()
}
