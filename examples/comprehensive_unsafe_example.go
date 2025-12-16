package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"unsafe"
	"encoding/binary"

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

	// Example: Process binary data from a network stream
	processNetworkData(chClient, writer)
}

// processNetworkData simulates processing binary data from a network stream
func processNetworkData(chClient *client.ClickHouseClient, writer *clickhouse.ColumnWriter) {
	fmt.Println("Processing binary data from network stream...")

	// Simulate receiving binary data from a network stream
	// This could be data from Kafka, a binary protocol, etc.
	
	// Example binary data structure:
	// 4 bytes: packet ID (uint32)
	// 8 bytes: timestamp (int64) 
	// 4 bytes: value (float32)
	// 8 bytes: counter (uint64)
	
	// Create sample binary data packets
	packets := createSamplePackets()
	
	// Extract data from binary packets
	packetIDs := make([]byte, 0)
	timestamps := make([]byte, 0)
	values := make([]byte, 0)
	counters := make([]byte, 0)
	
	for _, packet := range packets {
		if len(packet) >= 24 { // 4+8+4+8 = 24 bytes minimum
			packetIDs = append(packetIDs, packet[0:4]...)
			timestamps = append(timestamps, packet[4:12]...)
			values = append(values, packet[12:16]...)
			counters = append(counters, packet[16:24]...)
		}
	}
	
	// Convert byte slices to ClickHouse columns using unsafe pointer
	packetIDCol := writer.WriteUInt32ColumnFromBytes(packetIDs)
	timestampCol := writer.WriteInt64ColumnFromBytes(timestamps)
	valueCol := writer.WriteFloat32ColumnFromBytes(values)
	counterCol := writer.WriteUInt64ColumnFromBytes(counters)
	
	// Create the input block
	input := proto.Input{
		proto.InputColumn{Name: "packet_id", Data: packetIDCol},
		proto.InputColumn{Name: "timestamp", Data: timestampCol},
		proto.InputColumn{Name: "value", Data: valueCol},
		proto.InputColumn{Name: "counter", Data: counterCol},
	}
	
	// Insert the data
	ctx := context.Background()
	query := "INSERT INTO network_data_table (packet_id, timestamp, value, counter) VALUES"
	err := chClient.InsertBlock(ctx, query, input)
	if err != nil {
		log.Printf("Failed to insert network data: %v", err)
		return
	}
	
	fmt.Printf("Successfully inserted %d network data packets\n", len(packets))
}

// createSamplePackets creates sample binary data packets for demonstration
func createSamplePackets() [][]byte {
	packets := make([][]byte, 5)
	
	for i := 0; i < 5; i++ {
		packet := make([]byte, 24) // 24 bytes per packet
		
		// Packet ID (uint32)
		binary.LittleEndian.PutUint32(packet[0:4], uint32(1000+i))
		
		// Timestamp (int64)
		binary.LittleEndian.PutUint64(packet[4:12], uint64(time.Now().Unix()+int64(i)))
		
		// Value (float32)
		value := float32(1.5 + float64(i)*0.5)
		binary.LittleEndian.PutUint32(packet[12:16], *(*uint32)(unsafe.Pointer(&value)))
		
		// Counter (uint64)
		binary.LittleEndian.PutUint64(packet[16:24], uint64(10000+i))
		
		packets[i] = packet
	}
	
	return packets
}

// Example of how to handle mixed data types in a single binary structure
func processMixedDataStructure(chClient *client.ClickHouseClient, writer *clickhouse.ColumnWriter) {
	fmt.Println("Processing mixed data structure...")
	
	// Example: Binary structure with mixed types
	// Header: 2 bytes (uint16) - version
	// Field 1: 4 bytes (int32) - ID
	// Field 2: 8 bytes (float64) - measurement
	// Field 3: 1 byte (uint8) - status
	// Field 4: 4 bytes (uint32) - flags
	
	// Create sample mixed data structures
	structures := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		structure := make([]byte, 19) // 2+4+8+1+4 = 19 bytes
		
		// Version (uint16)
		binary.LittleEndian.PutUint16(structure[0:2], uint16(1))
		
		// ID (int32)
		binary.LittleEndian.PutUint32(structure[2:6], uint32(2000+i))
		
		// Measurement (float64)
		measurement := 10.5 + float64(i)*2.5
		binary.LittleEndian.PutUint64(structure[6:14], *(*uint64)(unsafe.Pointer(&measurement)))
		
		// Status (uint8)
		structure[14] = byte(1)
		
		// Flags (uint32)
		binary.LittleEndian.PutUint32(structure[15:19], uint32(0xF0F0))
		
		structures[i] = structure
	}
	
	// Extract individual fields from structures
	versions := make([]byte, 0)
	ids := make([]byte, 0)
	measurements := make([]byte, 0)
	statuses := make([]byte, 0)
	flags := make([]byte, 0)
	
	for _, structure := range structures {
		if len(structure) >= 19 {
			versions = append(versions, structure[0:2]...)
			ids = append(ids, structure[2:6]...)
			measurements = append(measurements, structure[6:14]...)
			statuses = append(statuses, structure[14:15]...)
			flags = append(flags, structure[15:19]...)
		}
	}
	
	// Convert to ClickHouse columns
	versionCol := writer.WriteUInt16ColumnFromBytes(versions)
	idCol := writer.WriteInt32ColumnFromBytes(ids)
	measurementCol := writer.WriteFloat64ColumnFromBytes(measurements)
	statusCol := writer.WriteUInt8ColumnFromBytes(statuses)
	flagCol := writer.WriteUInt32ColumnFromBytes(flags)
	
	// Create the input block
	input := proto.Input{
		proto.InputColumn{Name: "version", Data: versionCol},
		proto.InputColumn{Name: "id", Data: idCol},
		proto.InputColumn{Name: "measurement", Data: measurementCol},
		proto.InputColumn{Name: "status", Data: statusCol},
		proto.InputColumn{Name: "flags", Data: flagCol},
	}
	
	// Insert the data
	ctx := context.Background()
	query := "INSERT INTO mixed_data_table (version, id, measurement, status, flags) VALUES"
	err := chClient.InsertBlock(ctx, query, input)
	if err != nil {
		log.Printf("Failed to insert mixed data: %v", err)
		return
	}
	
	fmt.Printf("Successfully inserted %d mixed data structures\n", len(structures))
}