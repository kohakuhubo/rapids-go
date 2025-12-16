// main.go 是Berry Rapids Go项目的入口点，负责初始化和启动整个数据处理系统。
// 该文件实现了命令行接口、应用服务器和管理服务器的协调启动，以及优雅关闭机制。
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"berry-rapids-go/internal/app"
	"berry-rapids-go/internal/server"
	"github.com/spf13/cobra"
)

// httpPort 是管理API服务器监听的端口号，默认为8080
// dataSource 是数据源类型，默认为kafka
var (
	httpPort   int
	dataSource string
)

// main 函数是程序的入口点，负责设置命令行接口并启动服务器
func main() {
	// 创建根命令，定义程序的基本信息和运行逻辑
	var rootCmd = &cobra.Command{
		Use:   "berry-rapids-go",
		Short: "A high-performance data processing system for Kafka to ClickHouse",
		Long:  "A high-performance data processing system that reads data from Kafka, processes it, and writes to ClickHouse with aggregation capabilities.",
		Run: func(cmd *cobra.Command, args []string) {
			runServer()
		},
	}

	// 添加命令行参数，允许用户自定义HTTP管理端口
	rootCmd.Flags().IntVar(&httpPort, "http-port", 8080, "HTTP server port for management API")
	rootCmd.Flags().StringVar(&dataSource, "data-source", "kafka", "Data source type: kafka or nats")

	// 执行命令，如果出现错误则退出程序
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// runServer 函数负责启动应用服务器和管理服务器，并处理优雅关闭逻辑
func runServer() {
	fmt.Printf("Starting Berry Rapids Go server on HTTP port %d with data source %s...\n", httpPort, dataSource)

	// 创建应用服务器实例，负责核心的数据处理逻辑
	appServer := app.NewServer()
	
	// 创建HTTP管理服务器实例，提供健康检查、状态监控和远程控制功能
	mgmtServer := server.NewManagementServer(appServer, httpPort)

	// 在独立的goroutine中启动应用服务器，避免阻塞主线程
	go func() {
		// 在启动前设置数据源类型
		appServer.SetDataSource(dataSource)
		appServer.Start()
	}()

	// 在独立的goroutine中启动管理服务器，提供HTTP管理接口
	go func() {
		// 启动管理服务器，如果出现非预期错误则记录日志
		if err := mgmtServer.Start(); err != nil && err != http.ErrServerClosed {
			log.Printf("Management server error: %v", err)
		}
	}()

	// 设置信号通道，监听系统中断信号（SIGINT, SIGTERM）
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	// 阻塞等待中断信号
	<-sigChan

	fmt.Println("Shutting down Berry Rapids Go server...")
	
	// 创建带超时的上下文，确保关闭过程不会无限期阻塞
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	// 优雅关闭HTTP管理服务器
	if err := mgmtServer.Shutdown(ctx); err != nil {
		log.Printf("Management server shutdown error: %v", err)
	}
	
	// 关闭应用服务器
	appServer.Stop()
	
	fmt.Println("Server shutdown complete")
}