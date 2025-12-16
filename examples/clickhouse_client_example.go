package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"berry-rapids-go/internal/clickhouse/client"
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

	// Check server info
	ctx := context.Background()
	serverInfo, err := chClient.ServerInfo(ctx)
	if err != nil {
		log.Fatalf("Failed to get server info: %v", err)
	}
	fmt.Printf("Connected to ClickHouse server: %s\n", serverInfo.Name)

	// Example: Create a table
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS test_table (
			id UInt64,
			name String,
			value Float64,
			created_at DateTime
		) ENGINE = MergeTree()
		ORDER BY (id, created_at)
	`
	
	err = chClient.Query(ctx, createTableQuery, nil)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	fmt.Println("Table created successfully")

	// Example: Insert data using batch
	var (
		id        proto.ColUInt64
		name      proto.ColStr
		value     proto.ColFloat64
		createdAt proto.ColDateTime
	)

	// Add some sample data
	for i := 0; i < 1000; i++ {
		id.Append(uint64(i))
		name.Append(fmt.Sprintf("name_%d", i))
		value.Append(float64(i) * 0.5)
		createdAt.Append(time.Now())
	}

	// Prepare the input block
	input := proto.Input{
		{Name: "id", Data: &id},
		{Name: "name", Data: &name},
		{Name: "value", Data: &value},
		{Name: "created_at", Data: &createdAt},
	}

	// Insert the data
	insertQuery := "INSERT INTO test_table VALUES"
	err = chClient.InsertBlock(ctx, insertQuery, input)
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}
	fmt.Println("Data inserted successfully")

	// Example: Query data
	var (
		resultID        proto.ColUInt64
		resultName      proto.ColStr
		resultValue     proto.ColFloat64
		resultCreatedAt proto.ColDateTime
	)

	selectQuery := "SELECT id, name, value, created_at FROM test_table WHERE id < 10"
	err = chClient.Query(ctx, selectQuery, proto.Results{
		{Name: "id", Data: &resultID},
		{Name: "name", Data: &resultName},
		{Name: "value", Data: &resultValue},
		{Name: "created_at", Data: &resultCreatedAt},
	})
	
	if err != nil {
		log.Fatalf("Failed to query data: %v", err)
	}

	// Print results
	for i := 0; i < resultID.Rows(); i++ {
		fmt.Printf("ID: %d, Name: %s, Value: %f, Created At: %s\n",
			resultID.Row(i),
			resultName.Row(i),
			resultValue.Row(i),
			resultCreatedAt.Row(i).Format("2006-01-02 15:04:05"))
	}
}