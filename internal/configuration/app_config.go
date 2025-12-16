package configuration

import (
	"log"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// AppConfig holds the complete application configuration
type AppConfig struct {
	DataSource string           `mapstructure:"dataSource"` // 数据源类型：kafka 或 nats
	Kafka      KafkaConfig      `mapstructure:"kafka"`
	Nats       NatsConfig       `mapstructure:"nats"`
	ClickHouse ClickHouseConfig `mapstructure:"clickhouse"`
	Aggregate  AggregateConfig  `mapstructure:"aggregate"`
	Block      BlockConfig      `mapstructure:"block"`
	System     SystemConfig     `mapstructure:"system"`
}

// KafkaConfig holds Kafka configuration
type KafkaConfig struct {
	Brokers       []string      `mapstructure:"brokers"`
	Topic         string        `mapstructure:"topic"`
	ConsumerGroup string        `mapstructure:"consumerGroup"`
	BatchSize     int           `mapstructure:"batchSize"`
	PollTimeout   time.Duration `mapstructure:"pollTimeout"`
}

// NatsConfig holds NATS JetStream configuration
type NatsConfig struct {
	URL     string `mapstructure:"url"`     // NATS服务器地址
	Subject string `mapstructure:"subject"` // 主题名称
	Stream  string `mapstructure:"stream"`  // 流名称
}

// ClickHouseConfig holds ClickHouse configuration
type ClickHouseConfig struct {
	Host        string        `mapstructure:"host"`
	Port        int           `mapstructure:"port"`
	Database    string        `mapstructure:"database"`
	Username    string        `mapstructure:"username"`
	Password    string        `mapstructure:"password"`
	DialTimeout time.Duration `mapstructure:"dialTimeout"`
	Compress    bool          `mapstructure:"compress"`
}

// AggregateConfig holds aggregate processing configuration
type AggregateConfig struct {
	WaitTime    time.Duration `mapstructure:"waitTime"`
	ThreadSize  int           `mapstructure:"threadSize"`
	WaitQueue   int           `mapstructure:"waitQueue"`
	InsertQueue int           `mapstructure:"insertQueue"`
}

// SystemConfig holds system configuration
type SystemConfig struct {
	ParseThreadSize       int `mapstructure:"parseThreadSize"`
	DataInsertThreadSize  int `mapstructure:"dataInsertThreadSize"`
	DataInsertQueueLength int `mapstructure:"dataInsertQueueLength"`
}

// LoadConfig loads configuration from file
func LoadConfig() (*AppConfig, error) {
	viper.SetConfigName("app")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./configs")
	viper.AddConfigPath("../configs")
	viper.AddConfigPath("../../configs")

	// Set default values
	viper.SetDefault("dataSource", "kafka")
	viper.SetDefault("kafka.brokers", []string{"localhost:9092"})
	viper.SetDefault("kafka.topic", "berry-rapids-topic")
	viper.SetDefault("kafka.consumerGroup", "berry-rapids-consumer-group")
	viper.SetDefault("kafka.batchSize", 1000)
	viper.SetDefault("kafka.pollTimeout", "100ms")

	viper.SetDefault("nats.url", "nats://localhost:4222")
	viper.SetDefault("nats.subject", "berry-rapids-subject")
	viper.SetDefault("nats.stream", "berry-rapids-stream")

	viper.SetDefault("clickhouse.host", "localhost")
	viper.SetDefault("clickhouse.port", 9000)
	viper.SetDefault("clickhouse.database", "berry")
	viper.SetDefault("clickhouse.username", "default")
	viper.SetDefault("clickhouse.password", "honey")
	viper.SetDefault("clickhouse.dialTimeout", "10s")
	viper.SetDefault("clickhouse.compress", true)

	viper.SetDefault("aggregate.waitTime", "100ms")
	viper.SetDefault("aggregate.threadSize", 8)
	viper.SetDefault("aggregate.waitQueue", 64)
	viper.SetDefault("aggregate.insertQueue", 64)

	viper.SetDefault("block.batchDataMaxRowCnt", 10000)
	viper.SetDefault("block.batchDataMaxByteSize", 10485760) // 10MB
	viper.SetDefault("block.byteBufferFixedCacheSize", 100)
	viper.SetDefault("block.byteBufferDynamicCacheSize", 50)
	viper.SetDefault("block.blockSize", 8192)

	viper.SetDefault("system.parseThreadSize", 4)
	viper.SetDefault("system.dataInsertThreadSize", 4)
	viper.SetDefault("system.dataInsertQueueLength", 100)

	// Enable environment variable substitution
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		log.Printf("Warning: failed to read config file: %v. Using default values.", err)
	}

	var config AppConfig
	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}

	// Parse duration values
	if kafkaPollTimeout, err := time.ParseDuration(viper.GetString("kafka.pollTimeout")); err == nil {
		config.Kafka.PollTimeout = kafkaPollTimeout
	}

	if chDialTimeout, err := time.ParseDuration(viper.GetString("clickhouse.dialTimeout")); err == nil {
		config.ClickHouse.DialTimeout = chDialTimeout
	}

	if aggregateWaitTime, err := time.ParseDuration(viper.GetString("aggregate.waitTime")); err == nil {
		config.Aggregate.WaitTime = aggregateWaitTime
	}

	return &config, nil
}
