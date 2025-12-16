package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"berry-rapids-go/internal/clickhouse/client"
	"berry-rapids-go/pkg/clickhouse"
	"github.com/ClickHouse/ch-go/proto"
)

func main() {
	// Initialize ClickHouse client
	options := client.ClickHouseOptions{
		Host:        "localhost",
		Port:        9000,
		Database:    "default",
		Username:    "default",
		Password:    "",
		DialTimeout: 10 * time.Second,
	}

	chClient, err := client.NewClickHouseClient(options)
	if err != nil {
		log.Fatalf("Failed to create ClickHouse client: %v", err)
	}
	defer chClient.Close()

	// Create a ColumnWriter instance
	writer := clickhouse.NewColumnWriter()

	// Example 1: Write various data types to ClickHouse
	writeVariousDataTypes(chClient, writer)

	// Example 2: Write byte data directly to ClickHouse
	writeByteData(chClient, writer)

	// Example 3: Write structured data with multiple columns
	writeStructuredData(chClient, writer)
}

// writeVariousDataTypes demonstrates writing various ClickHouse data types
func writeVariousDataTypes(chClient *client.ClickHouseClient, writer *clickhouse.ColumnWriter) {
	fmt.Println("Example 1: Writing various data types to ClickHouse")

	// Prepare data for different columns
	stringData := []string{"apple", "banana", "cherry"}
	uint64Data := []uint64{100, 200, 300}
	float64Data := []float64{1.1, 2.2, 3.3}
	boolData := []bool{true, false, true}
	dateData := []time.Time{
		time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
	}

	// Create columns using ColumnWriter
	stringCol := writer.WriteStringColumn(stringData)
	uint64Col := writer.WriteUInt64Column(uint64Data)
	float64Col := writer.WriteFloat64Column(float64Data)
	boolCol := writer.WriteBoolColumn(boolData)
	dateCol := writer.WriteDateColumn(dateData)

	// Create the input block
	input := proto.Input{
		proto.InputColumn{Name: "name", Data: stringCol},
		proto.InputColumn{Name: "value", Data: uint64Col},
		proto.InputColumn{Name: "price", Data: float64Col},
		proto.InputColumn{Name: "active", Data: boolCol},
		proto.InputColumn{Name: "created_date", Data: dateCol},
	}

	// Insert the data
	ctx := context.Background()
	query := "INSERT INTO various_types_table (name, value, price, active, created_date) VALUES"
	err := chClient.InsertBlock(ctx, query, input)
	if err != nil {
		log.Printf("Failed to insert various data types: %v", err)
		return
	}

	fmt.Println("Successfully inserted various data types")
}

// writeByteData demonstrates writing byte data directly to ClickHouse
func writeByteData(chClient *client.ClickHouseClient, writer *clickhouse.ColumnWriter) {
	fmt.Println("Example 2: Writing byte data directly to ClickHouse")

	// Sample byte data (could be JSON, binary data, etc.)
	byteData := [][]byte{
		[]byte(`{"event": "login", "user_id": 123}`),
		[]byte(`{"event": "purchase", "product_id": 456}`),
		[]byte(`{"event": "logout", "user_id": 123}`),
	}

	// Write byte data as String column
	stringCol := writer.WriteStringColumnFromBytes(byteData)

	// Create the input block
	input := proto.Input{
		proto.InputColumn{Name: "data", Data: stringCol},
	}

	// Insert the data
	ctx := context.Background()
	query := "INSERT INTO byte_data_table (data) VALUES"
	err := chClient.InsertBlock(ctx, query, input)
	if err != nil {
		log.Printf("Failed to insert byte data: %v", err)
		return
	}

	fmt.Println("Successfully inserted byte data")
}

// writeStructuredData demonstrates writing structured data with multiple columns
func writeStructuredData(chClient *client.ClickHouseClient, writer *clickhouse.ColumnWriter) {
	fmt.Println("Example 3: Writing structured data with multiple columns")

	// Prepare structured data
	idData := []uint64{1, 2, 3}
	nameData := []string{"Alice", "Bob", "Charlie"}
	scoreData := []float64{95.5, 87.2, 92.8}
	activeData := []bool{true, true, false}
	timestampData := []time.Time{
		time.Now().Add(-2 * time.Hour),
		time.Now().Add(-1 * time.Hour),
		time.Now(),
	}

	// Create columns using ColumnWriter
	idCol := writer.WriteUInt64Column(idData)
	nameCol := writer.WriteStringColumn(nameData)
	scoreCol := writer.WriteFloat64Column(scoreData)
	activeCol := writer.WriteBoolColumn(activeData)
	timestampCol := writer.WriteDateTimeColumn(timestampData)

	// Create the input block
	input := proto.Input{
		proto.InputColumn{Name: "id", Data: idCol},
		proto.InputColumn{Name: "name", Data: nameCol},
		proto.InputColumn{Name: "score", Data: scoreCol},
		proto.InputColumn{Name: "active", Data: activeCol},
		proto.InputColumn{Name: "timestamp", Data: timestampCol},
	}

	// Insert the data
	ctx := context.Background()
	query := "INSERT INTO structured_data_table (id, name, score, active, timestamp) VALUES"
	err := chClient.InsertBlock(ctx, query, input)
	if err != nil {
		log.Printf("Failed to insert structured data: %v", err)
		return
	}

	fmt.Println("Successfully inserted structured data")
}