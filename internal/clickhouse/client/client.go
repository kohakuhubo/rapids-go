// client.go是Berry Rapids Go项目中ClickHouse客户端的核心实现文件。
// 该文件定义了ClickHouseClient结构体，提供了与ClickHouse数据库交互的TCP客户端功能。
// 设计原理：基于ch-go库实现高性能的ClickHouse TCP客户端，支持数据插入、查询和连接管理
// 实现方式：封装ch-go库的Client，提供简化的API接口，处理连接配置和错误处理
package client

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"time"
)

// ClickHouseClient表示一个ClickHouse TCP客户端
// 设计说明：该结构体封装了底层的ch-go客户端，提供简化的操作接口
type ClickHouseClient struct {
	conn    *ch.Client        // 底层ch-go客户端连接
	options ClickHouseOptions // 客户端配置选项
}

// ClickHouseOptions包含ClickHouse客户端的配置信息
// 设计原理：提供灵活的配置选项，支持各种连接参数和安全设置
type ClickHouseOptions struct {
	Host           string        // ClickHouse服务器主机地址
	Port           int           // ClickHouse服务器端口
	Database       string        // 数据库名称
	Username       string        // 用户名
	Password       string        // 密码
	DialTimeout    time.Duration // 连接超时时间
	Compress       bool          // 是否启用压缩
	TLSConfig      *tls.Config   // TLS配置（可选）
}

// NewClickHouseClient创建一个新的ClickHouse TCP客户端
// 设计原理：初始化客户端配置并建立与ClickHouse服务器的连接
// 参数说明：options - 客户端配置选项
// 返回值：ClickHouseClient实例和可能的错误
func NewClickHouseClient(options ClickHouseOptions) (*ClickHouseClient, error) {
	// 创建客户端实例
	client := &ClickHouseClient{
		options: options,
	}

	// 建立与ClickHouse服务器的连接
	if err := client.connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	// 返回客户端实例
	return client, nil
}

// connect建立与ClickHouse服务器的连接
// 设计原理：使用ch-go库的Dial方法建立TCP连接，配置连接参数
// 实现方式：构建ch.Options配置对象，调用ch.Dial建立连接
// 返回值：连接过程中可能的错误
func (c *ClickHouseClient) connect() error {
	// 构建连接选项
	opts := ch.Options{
		Address:          fmt.Sprintf("%s:%d", c.options.Host, c.options.Port), // 服务器地址
		Database:         c.options.Database,                                   // 数据库名称
		User:             c.options.Username,                                   // 用户名
		Password:         c.options.Password,                                   // 密码
		DialTimeout:      c.options.DialTimeout,                                // 连接超时时间
	}

	// 建立连接
	conn, err := ch.Dial(context.Background(), opts)
	if err != nil {
		return err
	}

	// 保存连接实例
	c.conn = conn
	return nil
}

// InsertBlock将数据块插入到ClickHouse中
// 设计原理：执行INSERT查询，将数据块写入指定表中
// 参数说明：ctx - 上下文，query - INSERT查询语句，block - 要插入的数据块
// 返回值：插入过程中可能的错误
func (c *ClickHouseClient) InsertBlock(ctx context.Context, query string, block proto.Input) error {
	// 执行INSERT查询
	return c.conn.Do(ctx, ch.Query{
		Body:  query,   // 查询语句
		Input: block,   // 输入数据块
	})
}

// InsertBlocks将多个数据块批量插入到ClickHouse中
// 设计原理：通过循环调用InsertBlock方法，实现批量数据插入
// 参数说明：ctx - 上下文，query - INSERT查询语句，blocks - 要插入的数据块列表
// 返回值：插入过程中可能的错误
func (c *ClickHouseClient) InsertBlocks(ctx context.Context, query string, blocks []proto.Input) error {
	// 遍历所有数据块并逐个插入
	for _, block := range blocks {
		if err := c.InsertBlock(ctx, query, block); err != nil {
			return fmt.Errorf("failed to insert block: %w", err)
		}
	}
	return nil
}

// Query执行SELECT查询并返回结果
// 设计原理：执行查询语句，将结果填充到指定的结果结构中
// 参数说明：ctx - 上下文，query - SELECT查询语句，result - 查询结果接收器
// 返回值：查询过程中可能的错误
func (c *ClickHouseClient) Query(ctx context.Context, query string, result proto.Results) error {
	// 执行SELECT查询
	return c.conn.Do(ctx, ch.Query{
		Body:   query,   // 查询语句
		Result: result,  // 结果接收器
	})
}

// Close关闭ClickHouse连接并释放相关资源
// 设计原理：确保连接正确关闭，避免资源泄漏
// 返回值：关闭过程中可能的错误
func (c *ClickHouseClient) Close() error {
	// 检查连接是否存在
	if c.conn != nil {
		// 关闭连接
		return c.conn.Close()
	}
	return nil
}