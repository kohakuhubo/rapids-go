# Berry Rapids Go Management API

## Overview

The management API provides HTTP endpoints for monitoring and controlling the Berry Rapids Go server.

## Endpoints

### Health Check
```
GET /health
```
Returns the health status of the server.

**Response:**
```json
{
  "status": "healthy",
  "time": "2025-10-30T10:00:00Z"
}
```

### Server Status
```
GET /status
```
Returns detailed server status information.

**Response:**
```json
{
  "status": "running",
  "start_time": "2025-10-30T10:00:00Z",
  "uptime": "2h30m15s",
  "port": 8080,
  "request_count": 1250,
  "version": "1.0.0",
  "components": {
    "kafka_consumer": "active",
    "clickhouse": "connected",
    "aggregator": "running"
  }
}
```

### Metrics
```
GET /metrics
```
Returns system and application metrics.

**Response:**
```json
{
  "timestamp": "2025-10-30T10:00:00Z",
  "metrics": {
    "cpu_usage": "15.2%",
    "memory_usage": "45.8%",
    "goroutines": "24",
    "batch_queue": "3",
    "kafka_lag": "0"
  },
  "throughput": {
    "messages_per_second": 1250,
    "bytes_per_second": "2.4 MB/s"
  }
}
```

### Graceful Shutdown
```
POST /shutdown
```
Initiates graceful shutdown of the server.

**Response:**
```json
{
  "status": "shutdown initiated",
  "time": "2025-10-30T10:00:00Z"
}
```

### Configuration
```
GET /config
```
Returns server configuration information.

**Response:**
```json
{
  "config": {
    "http_port": 8080,
    "version": "1.0.0"
  }
}
```

## Usage

Start the server with a custom HTTP port:
```bash
./berry-rapids-go --http-port 9090
```

Check server health:
```bash
curl http://localhost:8080/health
```

Get server status:
```bash
curl http://localhost:8080/status
```

View metrics:
```bash
curl http://localhost:8080/metrics
```

Initiate graceful shutdown:
```bash
curl -X POST http://localhost:8080/shutdown
```