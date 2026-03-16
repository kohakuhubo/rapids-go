// server.go 是Berry Rapids Go应用的核心服务器实现，负责协调所有组件的初始化、启动和关闭。
// 该文件实现了数据处理流水线的主循环，包括Kafka数据消费、批处理、聚合计算和ClickHouse数据持久化。
package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"berry-rapids-go/internal/aggregate"
	"berry-rapids-go/internal/clickhouse/client"
	"berry-rapids-go/internal/configuration"
	"berry-rapids-go/internal/data"
	"berry-rapids-go/internal/data/persistece"
	"berry-rapids-go/internal/data/source"
	"berry-rapids-go/internal/model"

	"go.uber.org/zap"
)

// Server 是应用的核心结构体，包含所有主要组件的引用
type Server struct {
	config            *configuration.AppConfig      // 应用配置
	logger            *zap.Logger                   // 日志记录器
	source            source.Source                 // 通用数据源
	clickhouse        *client.ClickHouseClient      // ClickHouse客户端
	aggregateServer   *aggregate.AggregateServer    // 数据聚合服务器
	batchProcessor    *data.BatchProcessor          // 批处理器
	memoryManager     *data.MemoryManager           // 内存管理器
	persistenceServer *persistece.PersistenceServer // 数据持久化服务器
	rowCounter        int64                         // 行计数器，用于生成rowID
	ctx               context.Context               // 上下文，用于协调goroutine
	cancel            context.CancelFunc            // 取消函数，用于优雅关闭
}

// NewServer 创建并初始化一个新的Server实例
// 该函数负责初始化日志记录器、加载配置和创建上下文
func NewServer() *Server {
	// 初始化生产环境日志记录器
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// 加载应用配置
	config, err := configuration.LoadConfig()
	if err != nil {
		logger.Fatal("Failed to load configuration", zap.Error(err))
	}

	// 创建可取消的上下文，用于协调所有goroutine
	ctx, cancel := context.WithCancel(context.Background())

	return &Server{
		config: config,
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start 启动服务器并运行主处理循环
// 该函数负责初始化所有组件、启动处理流水线，并等待中断信号
func (s *Server) Start() {
	s.logger.Info("Starting Berry Rapids Go server...")

	// 初始化所有核心组件
	if err := s.initializeComponents(); err != nil {
		s.logger.Fatal("Failed to initialize components", zap.Error(err))
		return
	}

	// 启动处理流水线
	if err := s.startProcessingPipeline(); err != nil {
		s.logger.Fatal("Failed to start processing pipeline", zap.Error(err))
		return
	}

	// 设置信号通道，监听系统中断信号（SIGINT, SIGTERM）
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 阻塞等待中断信号
	<-sigChan

	s.logger.Info("Received interrupt signal, shutting down server...")
}

// initializeComponents 初始化服务器的所有核心组件
// 该函数按照依赖关系顺序初始化组件，确保每个组件都能正确创建
func (s *Server) initializeComponents() error {
	// 初始化数据源
	var sourceConfig *source.DataSourceConfig

	// 根据配置选择数据源类型
	switch s.config.DataSource {
	case "kafka":
		sourceConfig = &source.DataSourceConfig{
			Type: source.KafkaDataSourceType,
			Kafka: &source.KafkaConfig{
				Brokers: s.config.Kafka.Brokers,
				Topic:   s.config.Kafka.Topic,
			},
		}
	case "nats":
		sourceConfig = &source.DataSourceConfig{
			Type: source.NatsDataSourceType,
			Nats: &source.NatsConfig{
				URL:     s.config.Nats.URL,
				Subject: s.config.Nats.Subject,
				Stream:  s.config.Nats.Stream,
			},
		}
	default:
		return fmt.Errorf("unsupported data source: %s", s.config.DataSource)
	}

	// 创建数据源实例
	dataSource, err := source.CreateDataSource(s.ctx, sourceConfig)
	if err != nil {
		return fmt.Errorf("failed to create data source: %w", err)
	}
	s.source = dataSource

	// 初始化ClickHouse客户端
	chOptions := client.ClickHouseOptions{
		Host:        s.config.ClickHouse.Host,
		Port:        s.config.ClickHouse.Port,
		Database:    s.config.ClickHouse.Database,
		Username:    s.config.ClickHouse.Username,
		Password:    s.config.ClickHouse.Password,
		DialTimeout: s.config.ClickHouse.DialTimeout,
		Compress:    s.config.ClickHouse.Compress,
	}

	clickhouse, err := client.NewClickHouseClient(chOptions)
	if err != nil {
		return fmt.Errorf("failed to create ClickHouse client: %w", err)
	}
	s.clickhouse = clickhouse

	// 初始化增强的ClickHouse客户端
	enhancedClickhouse := client.NewEnhancedClickHouseClient(s.clickhouse, s.logger)

	// 初始化内存管理器
	memoryManager := data.NewMemoryManager(
		s.logger,
		s.config.Block.GetByteBufferFixedCacheSize(),
		s.config.Block.GetByteBufferDynamicCacheSize(),
		s.config.Block.GetBlockSize(),
	)
	s.memoryManager = memoryManager

	// 初始化持久化服务器
	persistenceServer := persistece.NewPersistenceServer(s.logger, &s.config.Block, enhancedClickhouse)
	// 注册默认处理程序
	persistenceServer.RegisterHandler("default", "test_table")
	s.persistenceServer = persistenceServer

	// 初始化批处理器，使用管道架构提高性能
	batchProcessor := data.NewBatchProcessor(
		s.ctx, // 上下文，用于控制goroutine生命周期
		&s.config.Block,
		s.logger,
		5*time.Second, // 刷新间隔
		s.flushBatch,  // 刷新回调函数
		2,             // 管道工作线程数
	)
	s.batchProcessor = batchProcessor

	// 初始化聚合服务器
	aggregateConfig := &aggregate.Config{
		WaitTime:    s.config.Aggregate.WaitTime,
		QueueSize:   s.config.Aggregate.WaitQueue,
		WorkerCount: s.config.Aggregate.ThreadSize,
	}

	aggregateServer := aggregate.NewAggregateServer(s.logger, aggregateConfig)
	s.aggregateServer = aggregateServer

	s.logger.Info("All components initialized successfully")
	return nil
}

// startProcessingPipeline 启动数据处理流水线
// 该函数启动聚合服务器和数据处理循环
func (s *Server) startProcessingPipeline() error {
	// 启动聚合服务器
	if err := s.aggregateServer.Start(s.ctx); err != nil {
		return fmt.Errorf("failed to start aggregate server: %w", err)
	}

	// 在独立的goroutine中启动数据处理循环
	go s.processDataLoop()

	s.logger.Info("Processing pipeline started")
	return nil
}

// processDataLoop 是数据处理的主循环
// 该函数持续从数据源读取数据，进行批处理和聚合计算
func (s *Server) processDataLoop() {
	s.logger.Info("Starting data processing loop")

	// 无限循环处理数据，直到上下文被取消
	for {
		select {
		case <-s.ctx.Done():
			// 上下文被取消，停止数据处理循环
			s.logger.Info("Data processing loop stopped")
			return

		default:
			// 从数据源读取下一数据条目
			entry, err := s.source.Next()
			if err != nil {
				s.logger.Error("Failed to read from data source", zap.Error(err))
				continue
			}

			// 提取数据和rowID
			var data []byte
			var rowID int64

			// 尝试从条目中提取数据
			if dataGetter, ok := entry.(interface{ GetValue() []byte }); ok {
				data = dataGetter.GetValue()
			} else {
				s.logger.Error("Source entry does not implement GetValue method", zap.String("type", fmt.Sprintf("%T", entry)))
				continue
			}

			// 尝试解析MessagePack数据
			if parsedGetter, ok := entry.(interface {
				GetParsedValue() (map[string]interface{}, error)
			}); ok {
				if parsedData, err := parsedGetter.GetParsedValue(); err == nil {
					// 成功解析MessagePack数据
					s.logger.Debug("Successfully parsed MessagePack data", zap.Any("data", parsedData))
					// 您可以在这里处理解析后的数据
				} else {
					s.logger.Debug("Failed to parse MessagePack data, using raw data", zap.Error(err))
				}
			}

			// 尝试从条目中提取rowID
			if rowIDGetter, ok := entry.(interface{ GetRowId() int64 }); ok {
				rowID = rowIDGetter.GetRowId()
			} else if offsetGetter, ok := entry.(interface{ GetOffset() int64 }); ok {
				rowID = offsetGetter.GetOffset()
			} else {
				// 如果无法获取rowID，则使用计数器生成
				rowID = atomic.AddInt64(&s.rowCounter, 1)
			}

			// 将数据添加到批处理器
			if err := s.batchProcessor.AddData(data, rowID); err != nil {
				s.logger.Error("Failed to add data to batch processor", zap.Error(err))
			}

			// 创建批数据对象用于聚合计算
			// 创建 FieldBatch 对象
			fieldBatchData := make(map[string][][]byte)
			fieldBatchData["data"] = [][]byte{data}
			fieldBatch := model.NewFieldBatch(fieldBatchData)

			batchData := &model.BatchData{
				StartRowID:   rowID,
				EndRowID:     rowID,
				SizeInBytes:  int64(len(data)),
				NumberOfRows: 1,
				Content:      fieldBatch,
				DataSourceID: s.config.DataSource, // 设置数据源ID
			}

			// 将批数据转换为事件并发送到聚合服务器
			event := aggregate.NewBlockDataEvent(s.config.DataSource+"_message", batchData)
			if err := s.aggregateServer.Handle(s.ctx, event); err != nil {
				s.logger.Error("Failed to handle event", zap.Error(err))
			}
		}
	}
}

// flushBatch 是批处理器的回调函数，负责将批数据写入ClickHouse
// 该函数通过持久化服务器处理批数据
func (s *Server) flushBatch(batch *model.BatchData) error {
	// 如果批数据为空，则直接返回
	if batch.IsEmpty() {
		return nil
	}

	s.logger.Info("Flushing batch to ClickHouse",
		zap.Int64("startRowID", batch.StartRowID),
		zap.Int64("endRowID", batch.EndRowID),
		zap.Int64("sizeInBytes", batch.SizeInBytes),
		zap.Int64("numberOfRows", batch.NumberOfRows))

	// 使用持久化服务器处理批数据
	return s.persistenceServer.Handle(s.ctx, batch)
}

// Stop 优雅地关闭服务器及其所有组件
// 该函数按正确的顺序关闭所有组件，确保数据不会丢失
func (s *Server) Stop() {
	// 取消上下文，通知所有goroutine停止运行
	s.cancel()

	// 按顺序关闭组件，确保数据完整性
	if s.batchProcessor != nil {
		s.batchProcessor.Close()
	}

	if s.source != nil {
		s.source.Close()
	}

	if s.clickhouse != nil {
		s.clickhouse.Close()
	}

	if s.aggregateServer != nil {
		s.aggregateServer.Stop(s.ctx)
	}

	s.logger.Info("Server stopped")
}
