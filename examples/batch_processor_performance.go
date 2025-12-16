package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"sync"
	"sync/atomic"

	"berry-rapids-go/internal/configuration"
	"berry-rapids-go/internal/data"
	"berry-rapids-go/internal/model"
	"go.uber.org/zap"
)

func main() {
	// Initialize logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Initialize configuration
	config := configuration.NewBlockConfig()

	// Create a mock flush callback that simulates database write
	flushCallback := func(batch *model.BatchData) error {
		// Simulate database write delay
		time.Sleep(10 * time.Millisecond)
		fmt.Printf("Flushed batch: %d rows, %d bytes\n", batch.NumberOfRows, batch.SizeInBytes)
		return nil
	}

	// Test 1: Old synchronous approach
	fmt.Println("=== Testing synchronous approach ===")
	testSynchronousApproach(config, logger, flushCallback)

	// Test 2: New pipeline approach
	fmt.Println("\n=== Testing pipeline approach ===")
	testPipelineApproach(config, logger, flushCallback)
}

// testSynchronousApproach tests the old synchronous approach
func testSynchronousApproach(config *configuration.BlockConfig, logger *zap.Logger, flushCallback func(*model.BatchData) error) {
	// Create batch processor with 0 pipeline workers (synchronous)
	batchProcessor := data.NewBatchProcessor(
		context.Background(), // Context for batch processor
		config,
		logger,
		5*time.Second,
		flushCallback,
		0, // No pipeline workers
	)
	defer batchProcessor.Close()

	startTime := time.Now()
	var wg sync.WaitGroup
	var processedCount int64

	// Simulate data processing in multiple goroutines
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				data := []byte(fmt.Sprintf("data-%d-%d", workerID, j))
				rowID := int64(workerID*10 + j)
				
				if err := batchProcessor.AddData(data, rowID); err != nil {
					log.Printf("Error adding data: %v", err)
				}
				atomic.AddInt64(&processedCount, 1)
				
				// Small delay to simulate data arrival
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	
	// Force flush remaining data
	batchProcessor.Flush()
	
	duration := time.Since(startTime)
	fmt.Printf("Synchronous approach processed %d items in %v\n", processedCount, duration)
}

// testPipelineApproach tests the new pipeline approach
func testPipelineApproach(config *configuration.BlockConfig, logger *zap.Logger, flushCallback func(*model.BatchData) error) {
	// Create batch processor with pipeline workers
	batchProcessor := data.NewBatchProcessor(
		context.Background(), // Context for batch processor
		config,
		logger,
		5*time.Second,
		flushCallback,
		3, // 3 pipeline workers
	)
	defer batchProcessor.Close()

	startTime := time.Now()
	var wg sync.WaitGroup
	var processedCount int64

	// Simulate data processing in multiple goroutines
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				data := []byte(fmt.Sprintf("data-%d-%d", workerID, j))
				rowID := int64(workerID*10 + j)
				
				if err := batchProcessor.AddData(data, rowID); err != nil {
					log.Printf("Error adding data: %v", err)
				}
				atomic.AddInt64(&processedCount, 1)
				
				// Small delay to simulate data arrival
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	
	// Force flush remaining data
	batchProcessor.Flush()
	
	duration := time.Since(startTime)
	fmt.Printf("Pipeline approach processed %d items in %v\n", processedCount, duration)
}