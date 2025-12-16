package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"berry-rapids-go/internal/clickhouse/client"
	"github.com/ClickHouse/ch-go/proto"
)

// Example of how to write byte data directly to proto.Input
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

	// Example 1: Writing byte data as String column
	writeByteDataAsString(chClient)

	// Example 2: Writing byte data as raw bytes column
	writeByteDataAsBytes(chClient)

	// Example 3: Writing structured byte data with multiple columns
	writeStructuredByteData(chClient)
}

// writeByteDataAsString shows how to write byte data as String column in ClickHouse
func writeByteDataAsString(chClient *client.ClickHouseClient) {
	fmt.Println("Example 1: Writing byte data as String column")

	// Create a String column
	var colStr proto.ColStr

	// Sample byte data (could be JSON, binary data, etc.)
	byteData1 := []byte(`{"name": "John", "age": 30}`)
	byteData2 := []byte(`{"name": "Jane", "age": 25}`)
	byteData3 := []byte(`{"name": "Bob", "age": 35}`)

	// Append byte data as strings to the column
	colStr.Append(string(byteData1))
	colStr.Append(string(byteData2))
	colStr.Append(string(byteData3))

	// Create the input block
	input := proto.Input{
		proto.InputColumn{
			Name: "data",
			Data: &colStr,
		},
	}

	// Insert the data
	ctx := context.Background()
	query := "INSERT INTO byte_data_table (data) VALUES"
	err := chClient.InsertBlock(ctx, query, input)
	if err != nil {
		log.Printf("Failed to insert string data: %v", err)
		return
	}

	fmt.Println("Successfully inserted byte data as String column")
}

// writeByteDataAsBytes shows how to write byte data as raw bytes
func writeByteDataAsBytes(chClient *client.ClickHouseClient) {
	fmt.Println("Example 2: Writing byte data as raw bytes")

	// For raw bytes, we can use ColStr which can handle binary data
	var colBytes proto.ColStr

	// Sample binary data
	binaryData1 := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	binaryData2 := []byte{0x06, 0x07, 0x08, 0x09, 0x0A}
	binaryData3 := []byte{0x0B, 0x0C, 0x0D, 0x0E, 0x0F}

	// Append binary data as strings to the column
	// Note: In Go, []byte can be converted to string safely even with binary data
	colBytes.Append(string(binaryData1))
	colBytes.Append(string(binaryData2))
	colBytes.Append(string(binaryData3))

	// Create the input block
	input := proto.Input{
		proto.InputColumn{
			Name: "binary_data",
			Data: &colBytes,
		},
	}

	// Insert the data
	ctx := context.Background()
	query := "INSERT INTO binary_data_table (binary_data) VALUES"
	err := chClient.InsertBlock(ctx, query, input)
	if err != nil {
		log.Printf("Failed to insert binary data: %v", err)
		return
	}

	fmt.Println("Successfully inserted binary data as raw bytes")
}

// Example of how to handle structured byte data with multiple columns
func writeStructuredByteData(chClient *client.ClickHouseClient) {
	fmt.Println("Example 3: Writing structured byte data with multiple columns")

	// Create columns for different data types
	var (
		idCol   proto.ColUInt64
		nameCol proto.ColStr
		dataCol proto.ColStr // For byte data
	)

	// Sample structured data
	structuredData := []struct {
		id   uint64
		name string
		data []byte
	}{
		{1, "record1", []byte(`{"event": "login", "user_id": 123}`)},
		{2, "record2", []byte(`{"event": "logout", "user_id": 123}`)},
		{3, "record3", []byte(`{"event": "purchase", "user_id": 456}`)},
	}

	// Populate columns
	for _, record := range structuredData {
		idCol.Append(record.id)
		nameCol.Append(record.name)
		dataCol.Append(string(record.data)) // Convert byte slice to string
	}

	// Create the input block with multiple columns
	input := proto.Input{
		proto.InputColumn{
			Name: "id",
			Data: &idCol,
		},
		proto.InputColumn{
			Name: "name",
			Data: &nameCol,
		},
		proto.InputColumn{
			Name: "data",
			Data: &dataCol,
		},
	}

	// Insert the data
	ctx := context.Background()
	query := "INSERT INTO structured_data_table (id, name, data) VALUES"
	err := chClient.InsertBlock(ctx, query, input)
	if err != nil {
		log.Printf("Failed to insert structured data: %v", err)
		return
	}

	fmt.Println("Successfully inserted structured byte data with multiple columns")
}