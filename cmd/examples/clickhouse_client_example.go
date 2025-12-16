package main

import (
	"berry-rapids-go/internal/clickhouse/client"
	"berry-rapids-go/pkg/clickhouse"
	"context"
	"fmt"
	"log"
	"time"
)

func main() {
	// Configure ClickHouse client options
	options := client.ClickHouseOptions{
		Host:        "localhost",
		Port:        9000,
		Database:    "default",
		Username:    "default",
		Password:    "",
		DialTimeout: 10 * time.Second,
		Compress:    true,
	}

	// Create a new ClickHouse client
	chClient, err := client.NewClickHouseClient(options)
	if err != nil {
		log.Fatalf("Failed to create ClickHouse client: %v", err)
	}
	defer chClient.Close()

	// Create a batch inserter
	batchInserter := clickhouse.NewBatchInserter(chClient, "test_table", 1000)
	
	// Set columns for the buffer
	batchInserter.SetColumns([]string{"id", "name", "value", "created_at"})

	// Add some sample data
	for i := 0; i < 2500; i++ {
		values := []interface{}{
			uint64(i),
			fmt.Sprintf("name_%d", i),
			float64(i) * 0.5,
			time.Now(),
		}
		
		if err := batchInserter.AddRow(values); err != nil {
			log.Printf("Failed to add row: %v", err)
		}
	}

	// Flush any remaining data
	ctx := context.Background()
	if err := batchInserter.Flush(ctx); err != nil {
		log.Printf("Failed to flush data: %v", err)
	}

	fmt.Println("Data inserted successfully")
}