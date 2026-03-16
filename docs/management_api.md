# Berry Rapids Go 管理 API

## 概述

管理 API 提供用于监控和控制 Berry Rapids Go 服务器的 HTTP 端点。

## 端点

### 健康检查
```
GET /health
```
返回服务器的健康状态。

**响应：**
```json
{
  "status": "healthy",
  "time": "2025-10-30T10:00:00Z"
}
```

### 服务器状态
```
GET /status
```
返回详细的服务器状态信息。

**响应：**
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

### 指标
```
GET /metrics
```
返回系统和应用程序指标。

**响应：**
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

### 优雅关闭
```
POST /shutdown
```
启动服务器的优雅关闭。

**响应：**
```json
{
  "status": "shutdown initiated",
  "time": "2025-10-30T10:00:00Z"
}
```

### 配置
```
GET /config
```
返回服务器配置信息。

**响应：**
```json
{
  "config": {
    "http_port": 8080,
    "version": "1.0.0"
  }
}
```

## 使用方法

使用自定义 HTTP 端口启动服务器：
```bash
./berry-rapids-go --http-port 9090
```

检查服务器健康状态：
```bash
curl http://localhost:8080/health
```

获取服务器状态：
```bash
curl http://localhost:8080/status
```

查看指标：
```bash
curl http://localhost:8080/metrics
```

启动优雅关闭：
```bash
curl -X POST http://localhost:8080/shutdown
```