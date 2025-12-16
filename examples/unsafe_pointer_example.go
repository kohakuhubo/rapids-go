package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"encoding/binary"
	"unsafe"

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

	// Example 1: Write uint32 data from byte slice
	writeUInt32FromBytes(chClient, writer)

	// Example 2: Write float64 data from byte slice
	writeFloat64FromBytes(chClient, writer)

	// Example 3: Write multiple data types from byte slices
	writeMultipleTypesFromBytes(chClient, writer)
}

// writeUInt32FromBytes demonstrates writing uint32 data from byte slice
func writeUInt32FromBytes(chClient *client.ClickHouseClient, writer *clickhouse.ColumnWriter) {
	fmt.Println("Example 1: Writing uint32 data from byte slice")

	// Create sample uint32 data and convert to byte slice
	uint32Data := []uint32{100, 200, 300, 400, 500}
	byteData := make([]byte, len(uint32Data)*4)
	for i, v := range uint32Data {
		binary.LittleEndian.PutUint32(byteData[i*4:], v)
	}

	// Write uint32 data from byte slice using unsafe pointer
	uint32Col := writer.WriteUInt32ColumnFromBytes(byteData)

	// Create the input block
	input := proto.Input{
		proto.InputColumn{Name: "value", Data: uint32Col},
	}

	// Insert the data
	ctx := context.Background()
	query := "INSERT INTO uint32_data_table (value) VALUES"
	err := chClient.InsertBlock(ctx, query, input)
	if err != nil {
		log.Printf("Failed to insert uint32 data: %v", err)
		return
	}

	fmt.Println("Successfully inserted uint32 data from byte slice")
}

// writeFloat64FromBytes demonstrates writing float64 data from byte slice
func writeFloat64FromBytes(chClient *client.ClickHouseClient, writer *clickhouse.ColumnWriter) {
	fmt.Println("Example 2: Writing float64 data from byte slice")

	// Create sample float64 data and convert to byte slice
	float64Data := []float64{1.1, 2.2, 3.3, 4.4, 5.5}
	byteData := make([]byte, len(float64Data)*8)
	for i, v := range float64Data {
		binary.LittleEndian.PutUint64(byteData[i*8:], *(*uint64)(unsafe.Pointer(&v)))
	}

	// Write float64 data from byte slice using unsafe pointer
	float64Col := writer.WriteFloat64ColumnFromBytes(byteData)

	// Create the input block
	input := proto.Input{
		proto.InputColumn{Name: "value", Data: float64Col},
	}

	// Insert the data
	ctx := context.Background()
	query := "INSERT INTO float64_data_table (value) VALUES"
	err := chClient.InsertBlock(ctx, query, input)
	if err != nil {
		log.Printf("Failed to insert float64 data: %v", err)
		return
	}

	fmt.Println("Successfully inserted float64 data from byte slice")
}

// writeMultipleTypesFromBytes demonstrates writing multiple data types from byte slices
func writeMultipleTypesFromBytes(chClient *client.ClickHouseClient, writer *clickhouse.ColumnWriter) {
	fmt.Println("Example 3: Writing multiple data types from byte slices")

	// Create sample data for different types
	// UInt64 data
	uint64Data := []uint64{1000, 2000, 3000}
	uint64Bytes := make([]byte, len(uint64Data)*8)
	for i, v := range uint64Data {
		binary.LittleEndian.PutUint64(uint64Bytes[i*8:], v)
	}

	// Int32 data
	int32Data := []int32{-100, -200, -300}
	int32Bytes := make([]byte, len(int32Data)*4)
	for i, v := range int32Data {
		binary.LittleEndian.PutUint32(int32Bytes[i*4:], uint32(v))
	}

	// Float32 data
	float32Data := []float32{1.5, 2.5, 3.5}
	float32Bytes := make([]byte, len(float32Data)*4)
	for i, v := range float32Data {
		binary.LittleEndian.PutUint32(float32Bytes[i*4:], *(*uint32)(unsafe.Pointer(&v)))
	}

	// Write data from byte slices using unsafe pointer
	uint64Col := writer.WriteUInt64ColumnFromBytes(uint64Bytes)
	int32Col := writer.WriteInt32ColumnFromBytes(int32Bytes)
	float32Col := writer.WriteFloat32ColumnFromBytes(float32Bytes)

	// Create the input block
	input := proto.Input{
		proto.InputColumn{Name: "uint64_value", Data: uint64Col},
		proto.InputColumn{Name: "int32_value", Data: int32Col},
		proto.InputColumn{Name: "float32_value", Data: float32Col},
	}

	// Insert the data
	ctx := context.Background()
	query := "INSERT INTO multiple_types_table (uint64_value, int32_value, float32_value) VALUES"
	err := chClient.InsertBlock(ctx, query, input)
	if err != nil {
		log.Printf("Failed to insert multiple types data: %v", err)
		return
	}

	fmt.Println("Successfully inserted multiple data types from byte slices")
}