# 数据源配置指南

## 概述

Berry Rapids Go支持多种数据源，包括Kafka和NATS JetStream。通过配置文件或命令行参数，可以轻松切换不同的数据源。

## 配置方式

### 1. 通过配置文件

在`configs/app.yaml`中设置数据源类型：

```yaml
# 数据源类型：kafka 或 nats
dataSource: kafka

# Kafka配置
kafka:
  brokers:
    - "localhost:9092"
  topic: "berry-rapids-topic"

# NATS JetStream配置
nats:
  url: "nats://localhost:4222"
  subject: "berry-rapids-subject"
  stream: "berry-rapids-stream"
```

### 2. 通过命令行参数

使用`--data-source`参数指定数据源类型：

```bash
# 使用Kafka数据源
./berry-rapids-go --data-source=kafka

# 使用NATS JetStream数据源
./berry-rapids-go --data-source=nats
```

## 数据源类型

### Kafka

Kafka是最常用的数据源，适用于高吞吐量的消息处理场景。

配置示例：
```yaml
dataSource: kafka
kafka:
  brokers:
    - "localhost:9092"
  topic: "berry-rapids-topic"
```

### NATS JetStream

NATS JetStream是一个轻量级、高性能的消息系统，适用于需要低延迟和高可靠性的场景。

配置示例：
```yaml
dataSource: nats
nats:
  url: "nats://localhost:4222"
  subject: "berry-rapids-subject"
  stream: "berry-rapids-stream"
```

## 扩展新的数据源

要添加新的数据源类型，请按照以下步骤操作：

1. 在`internal/data/source/`目录下创建新的数据源目录
2. 实现`Source`接口
3. 实现`SourceEntry`接口
4. 在`factory.go`中添加数据源类型和创建逻辑
5. 在配置结构体中添加相应的配置字段

## 注意事项

1. 确保所选数据源的服务正在运行
2. 根据数据源类型正确配置相关参数
3. 在生产环境中，建议使用配置文件而不是命令行参数来管理配置