# Berry Rapids Go

Berry Rapids Go is a high-performance data processing system that reads data from Kafka, processes it, and writes to ClickHouse with aggregation capabilities.

## Features

- High-performance data processing pipeline
- Multiple data source support (Kafka, NATS JetStream)
- ClickHouse data persistence
- Data aggregation capabilities
- Batch processing with memory management
- Configurable through YAML files

## Prerequisites

- Go 1.21 or higher
- Kafka cluster (for Kafka data source) or NATS server (for NATS JetStream data source)
- ClickHouse database

## Configuration

The application is configured through the `configs/app.yaml` file. You can modify the following sections:

### Data Source Configuration
- `dataSource`: Type of data source ('kafka' or 'nats')
- `kafka`: Kafka-specific configuration
- `nats`: NATS JetStream-specific configuration

### Kafka Configuration
- `brokers`: List of Kafka broker addresses
- `topic`: Kafka topic to consume from
- `consumerGroup`: Consumer group ID
- `batchSize`: Number of messages to fetch in each batch
- `pollTimeout`: Timeout for Kafka poll operations

### NATS JetStream Configuration
- `url`: NATS server URL
- `subject`: Subject to consume from
- `stream`: Stream name

### ClickHouse Configuration
- `host`: ClickHouse server host
- `port`: ClickHouse server port
- `database`: Database name
- `username`: Username for authentication
- `password`: Password for authentication
- `dialTimeout`: Connection timeout
- `compress`: Enable compression

### Aggregation Configuration
- `waitTime`: Time to wait before processing aggregated data
- `threadSize`: Number of worker threads for aggregation
- `waitQueue`: Size of the wait queue
- `insertQueue`: Size of the insert queue

### Block Processing Configuration
- `batchDataMaxRowCnt`: Maximum number of rows per batch
- `batchDataMaxByteSize`: Maximum size of batch in bytes
- `byteBufferFixedCacheSize`: Size of fixed buffer cache
- `byteBufferDynamicCacheSize`: Size of dynamic buffer cache
- `blockSize`: Size of data blocks

### System Configuration
- `parseThreadSize`: Number of threads for parsing
- `dataInsertThreadSize`: Number of threads for data insertion
- `dataInsertQueueLength`: Length of data insertion queue

## Building

```bash
go build -o berry-rapids-go
```

## Running

```bash
# Use Kafka data source (default)
./berry-rapids-go

# Use NATS JetStream data source
./berry-rapids-go --data-source=nats

# Specify HTTP management port
./berry-rapids-go --http-port=9090
```

## Testing

```bash
go test ./...
```