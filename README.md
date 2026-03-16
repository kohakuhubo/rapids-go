，# Berry Rapids Go

Berry Rapids Go 是一个高性能数据处理系统，从 Kafka 读取数据，进行处理，并写入 ClickHouse，具有聚合功能。

## 功能特性

- 高性能数据处理管道
- 多数据源支持（Kafka、NATS JetStream）
- ClickHouse 数据持久化
- 数据聚合功能
- 带内存管理的批处理
- 通过 YAML 文件配置

## 前置要求

- Go 1.21 或更高版本
- Kafka 集群（用于 Kafka 数据源）或 NATS 服务器（用于 NATS JetStream 数据源）
- ClickHouse 数据库

## 配置

应用程序通过 `configs/app.yaml` 文件进行配置。您可以修改以下部分：

### 数据源配置
- `dataSource`: 数据源类型（'kafka' 或 'nats'）
- `kafka`: Kafka 特定配置
- `nats`: NATS JetStream 特定配置

### Kafka 配置
- `brokers`: Kafka 代理地址列表
- `topic`: 要消费的 Kafka 主题
- `consumerGroup`: 消费者组 ID
- `batchSize`: 每批获取的消息数量
- `pollTimeout`: Kafka 轮询操作的超时时间

### NATS JetStream 配置
- `url`: NATS 服务器 URL
- `subject`: 要消费的主题
- `stream`: 流名称

### ClickHouse 配置
- `host`: ClickHouse 服务器主机
- `port`: ClickHouse 服务器端口
- `database`: 数据库名称
- `username`: 认证用户名
- `password`: 认证密码
- `dialTimeout`: 连接超时
- `compress`: 启用压缩

### 聚合配置
- `waitTime`: 处理聚合数据前等待的时间
- `threadSize`: 聚合的工作线程数
- `waitQueue`: 等待队列的大小
- `insertQueue`: 插入队列的大小

### 块处理配置
- `batchDataMaxRowCnt`: 每批的最大行数
- `batchDataMaxByteSize`: 批的最大字节数
- `byteBufferFixedCacheSize`: 固定缓冲区缓存大小
- `byteBufferDynamicCacheSize`: 动态缓冲区缓存大小
- `blockSize`: 数据块大小

### 系统配置
- `parseThreadSize`: 解析线程数
- `dataInsertThreadSize`: 数据插入线程数
- `dataInsertQueueLength`: 数据插入队列长度

## 构建

```bash
go build -o berry-rapids-go
```

## 运行

```bash
# 使用 Kafka 数据源（默认）
./berry-rapids-go

# 使用 NATS JetStream 数据源
./berry-rapids-go --data-source=nats

# 指定 HTTP 管理端口
./berry-rapids-go --http-port=9090
```

## 测试

```bash
go test ./...
```