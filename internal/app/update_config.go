// update_config.go是Berry Rapids Go项目中用于更新服务器配置的文件。
// 该文件提供了在运行时更新服务器配置的方法。
package app

// SetDataSource设置数据源类型
// 该方法允许在服务器启动前更新数据源类型配置
// 参数说明：dataSource - 数据源类型（"kafka" 或 "nats"）
func (s *Server) SetDataSource(dataSource string) {
	if s.config != nil {
		s.config.DataSource = dataSource
	}
}