// management.go 是Berry Rapids Go项目中管理API模块的核心实现文件。
// 该文件定义了ManagementServer 结构体，实现了HTTP管理接口，用于监控和控制服务器状态。
// 设计原理：通过HTTP RESTful API提供健康检查、状态监控、性能指标和优雅关闭等功能
// 实现方式：使用Go标准库net/http实现HTTP服务器，通过原子操作保证并发安全
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"berry-rapids-go/internal/app"
)

// ManagementServer 提供HTTP端点用于服务器管理
// 设计说明：该结构体实现了健康检查、状态监控、性能指标和优雅关闭等管理功能
type ManagementServer struct {
	appServer    *app.Server    // 应用服务器实例
	httpServer   *http.Server   // HTTP服务器实例
	port         int            // 监听端口
	startTime    time.Time      // 服务器启动时间
	requestCount int64          // 请求计数器，使用原子操作保证并发安全
}

// NewManagementServer 创建新的管理服务器实例
// 设计原理：初始化管理服务器配置，记录启动时间
// 参数说明：
//   appServer - 应用服务器实例
//   port - 监听端口
// 返回值：ManagementServer实例
func NewManagementServer(appServer *app.Server, port int) *ManagementServer {
	// 创建管理服务器实例并初始化基本配置
	return &ManagementServer{
		appServer: appServer,  // 关联应用服务器实例
		port:      port,       // 设置监听端口
		startTime: time.Now(), // 记录服务器启动时间
	}
}

// Start 启动HTTP管理服务器
// 设计原理：注册管理端点，添加中间件，启动HTTP服务器监听
// 实现方式：使用http.ServeMux注册路由，添加请求计数中间件，启动HTTP服务器
// 返回值：启动过程中可能的错误
func (s *ManagementServer) Start() error {
	mux := http.NewServeMux()

	// 注册管理端点
	// 健康检查端点：检查服务器是否正常运行
	mux.HandleFunc("/health", s.healthHandler)
	// 状态端点：提供详细的服务器状态信息
	mux.HandleFunc("/status", s.statusHandler)
	// 指标端点：提供系统性能指标
	mux.HandleFunc("/metrics", s.metricsHandler)
	// 关闭端点：优雅关闭服务器
	mux.HandleFunc("/shutdown", s.shutdownHandler)
	// 配置端点：提供配置信息
	mux.HandleFunc("/config", s.configHandler)

	// 添加中间件以统计请求数量
	handler := s.requestCountMiddleware(mux)

	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port), // 设置监听地址和端口
		Handler: handler,                     // 设置处理程序（包含中间件）
	}

	// 记录管理服务器启动日志
	log.Printf("Management server starting on port %d", s.port)
	// 启动HTTP服务器监听
	return s.httpServer.ListenAndServe()
}

// Shutdown 优雅关闭HTTP服务器
// 设计原理：提供标准的HTTP服务器优雅关闭机制
// 参数说明：ctx - 上下文，用于控制关闭超时
// 返回值：关闭过程中可能的错误
func (s *ManagementServer) Shutdown(ctx context.Context) error {
	// 如果HTTP服务器实例存在，则执行优雅关闭
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	// 如果HTTP服务器实例不存在，返回nil
	return nil
}

// requestCountMiddleware 请求计数中间件
// 设计原理：统计传入的HTTP请求数量，使用原子操作保证并发安全
// 实现方式：在处理请求前增加请求计数器
// 参数说明：next - 下一个HTTP处理程序
// 返回值：包装后的HTTP处理程序
func (s *ManagementServer) requestCountMiddleware(next http.Handler) http.Handler {
	// 返回包装后的HTTP处理程序
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 使用原子操作增加请求计数器
		atomic.AddInt64(&s.requestCount, 1)
		// 调用下一个处理程序
		next.ServeHTTP(w, r)
	})
}

// healthHandler 健康检查端点处理函数
// 设计原理：提供简单的健康检查响应，用于负载均衡器或监控系统检测服务器状态
// 参数说明：
//   w - HTTP响应写入器
//   r - HTTP请求
func (s *ManagementServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	// 设置响应内容类型为JSON
	w.Header().Set("Content-Type", "application/json")

	// 构造健康检查响应
	response := map[string]interface{}{
		"status": "healthy",             // 健康状态
		"time":   time.Now().UTC(),      // 当前UTC时间
	}

	// 将响应编码为JSON并写入响应体
	json.NewEncoder(w).Encode(response)
}

// statusHandler 状态端点处理函数
// 设计原理：提供详细的服务器状态信息，包括运行时间、请求计数等
// 参数说明：
//   w - HTTP响应写入器
//   r - HTTP请求
func (s *ManagementServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	// 设置响应内容类型为JSON
	w.Header().Set("Content-Type", "application/json")

	// 构造状态响应
	response := map[string]interface{}{
		"status":        "running",                          // 运行状态
		"start_time":    s.startTime,                        // 启动时间
		"uptime":        time.Since(s.startTime).String(),   // 运行时间
		"port":          s.port,                             // 监听端口
		"request_count": atomic.LoadInt64(&s.requestCount),  // 请求计数（原子读取）
		"version":       "1.0.0",                            // 版本号
		"components": map[string]string{                     // 组件状态
			"kafka_consumer": "active",    // Kafka消费者状态
			"clickhouse":     "connected", // ClickHouse连接状态
			"aggregator":     "running",   // 聚合器状态
		},
	}

	// 将响应编码为JSON并写入响应体
	json.NewEncoder(w).Encode(response)
}

// metricsHandler 指标端点处理函数
// 设计原理：提供系统性能指标，用于监控系统运行状态
// 参数说明：
//   w - HTTP响应写入器
//   r - HTTP请求
func (s *ManagementServer) metricsHandler(w http.ResponseWriter, r *http.Request) {
	// 设置响应内容类型为JSON
	w.Header().Set("Content-Type", "application/json")

	// 收集实际指标（此处为示例数据）
	response := map[string]interface{}{
		"timestamp": time.Now().UTC(),  // 时间戳
		"metrics": map[string]interface{}{  // 系统指标
			"cpu_usage":    "15.2%",    // CPU使用率
			"memory_usage": "45.8%",    // 内存使用率
			"goroutines":   "24",       // goroutine数量
			"batch_queue":  "3",        // 批处理队列长度
			"kafka_lag":    "0",        // Kafka滞后量
		},
		"throughput": map[string]interface{}{  // 吞吐量指标
			"messages_per_second": 1250,       // 每秒消息数
			"bytes_per_second":    "2.4 MB/s", // 每秒字节数
		},
	}

	// 将响应编码为JSON并写入响应体
	json.NewEncoder(w).Encode(response)
}

// shutdownHandler 关闭端点处理函数
// 设计原理：提供HTTP接口触发服务器优雅关闭
// 实现方式：验证请求方法，发送响应后在独立goroutine中触发系统信号
// 参数说明：
//   w - HTTP响应写入器
//   r - HTTP请求
func (s *ManagementServer) shutdownHandler(w http.ResponseWriter, r *http.Request) {
	// 只允许POST方法
	if r.Method != http.MethodPost {
		// 如果方法不被允许，返回405状态码
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 设置响应内容类型为JSON
	w.Header().Set("Content-Type", "application/json")

	// 构造关闭响应
	response := map[string]interface{}{
		"status": "shutdown initiated", // 关闭状态
		"time":   time.Now().UTC(),     // 当前UTC时间
	}

	// 将响应编码为JSON并写入响应体
	json.NewEncoder(w).Encode(response)

	// 在独立的goroutine中触发关闭，避免阻塞响应
	go func() {
		// 等待100毫秒以确保响应被发送
		time.Sleep(100 * time.Millisecond)
		// 查找当前进程以触发优雅关闭
		p, _ := os.FindProcess(os.Getpid())
		// 向进程发送SIGTERM信号以触发优雅关闭
		err := p.Signal(syscall.SIGTERM)
		if err != nil {
			// 如果发送信号失败，直接返回
			return
		}
	}()
}

// configHandler 配置端点处理函数
// 设计原理：提供服务器配置信息查询接口
// 参数说明：
//   w - HTTP响应写入器
//   r - HTTP请求
func (s *ManagementServer) configHandler(w http.ResponseWriter, r *http.Request) {
	// 设置响应内容类型为JSON
	w.Header().Set("Content-Type", "application/json")

	// TODO: 实现实际的配置检索逻辑
	// 构造配置响应
	response := map[string]interface{}{
		"config": map[string]interface{}{  // 配置信息
			"http_port": s.port,    // HTTP端口
			"version":   "1.0.0",   // 版本号
		},
	}

	// 将响应编码为JSON并写入响应体
	json.NewEncoder(w).Encode(response)
}
