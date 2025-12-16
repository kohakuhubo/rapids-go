package main

import (
	"context"
	"fmt"
	"time"
	"berry-rapids-go/internal/model"
	"berry-rapids-go/internal/configuration"
	"berry-rapids-go/internal/data"
	"berry-rapids-go/internal/data/persistece"
	"berry-rapids-go/internal/clickhouse/client"
	"go.uber.org/zap"
)

func main() {
	// Initialize logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Create configuration
	config := configuration.NewBlockConfig()
	
	// Create memory manager
	memoryManager := data.NewMemoryManager(
		logger,
		config.GetByteBufferFixedCacheSize(),
		config.GetByteBufferDynamicCacheSize(),
		config.GetBlockSize(),
	)
	
	// Create ClickHouse client (simplified for example)
	clickhouseOpts := client.ClickHouseOptions{
		Host:        "localhost",
		Port:        9000,
		Database:    "default",
		Username:    "default",
		Password:    "",
		DialTimeout: 10 * time.Second,
		Compress:    true,
	}
	
	chClient, err := client.NewClickHouseClient(clickhouseOpts)
	if err != nil {
		logger.Fatal("Failed to create ClickHouse client", zap.Error(err))
	}
	
	enhancedClient := client.NewEnhancedClickHouseClient(chClient, logger)
	
	// Create persistence server
	persistenceServer := persistece.NewPersistenceServer(logger, config, enhancedClient)
	persistenceServer.RegisterHandler("default", "test_table")
	
	// Create batch processor
	batchProcessor := data.NewBatchProcessor(
		context.Background(), // Context for batch processor
		config,
		logger,
		5*time.Second, // Flush interval
		func(batch *model.BatchData) error {
			return persistenceServer.Handle(context.Background(), batch)
		},
		2, // Pipeline workers
	)
	
	// Simulate data processing
	fmt.Println("Starting batch processing example...")
	
	// Generate and process some sample data
	for i := 0; i < 100; i++ {
		// Create sample data
		data := []byte(fmt.Sprintf("Sample data row %d", i))
		
		// Add data to batch processor
		if err := batchProcessor.AddData(data, int64(i)); err != nil {
			logger.Error("Failed to add data to batch", zap.Error(err))
		}
		
		// Print memory stats every 20 rows
		if i%20 == 0 {
			stats := memoryManager.GetStats()
			fmt.Printf("Memory stats - Allocated: %d bytes, Peak: %d bytes\n",
				stats["allocatedBytes"], stats["peakMemoryUsage"])
		}
		
		// Small delay to simulate data arrival
		time.Sleep(100 * time.Millisecond)
	}
	
	// Force flush remaining data
	if err := batchProcessor.Flush(); err != nil {
		logger.Error("Failed to flush batch processor", zap.Error(err))
	}
	
	// Close batch processor
	if err := batchProcessor.Close(); err != nil {
		logger.Error("Failed to close batch processor", zap.Error(err))
	}
	
	fmt.Println("Batch processing example completed")
	
	// Print final memory stats
	stats := memoryManager.GetStats()
	fmt.Printf("Final memory stats - Allocated: %d bytes, Peak: %d bytes\n",
		stats["allocatedBytes"], stats["peakMemoryUsage"])
}