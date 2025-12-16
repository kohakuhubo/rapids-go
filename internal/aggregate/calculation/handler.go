package aggregate

import "berry-rapids-go/internal/aggregate"

// CalculationHandler 计算处理器接口
type CalculationHandler interface {
	// Type 返回处理器处理的事件类型
	Type() string

	// Handle 处理区块数据事件，返回聚合后的结果
	Handle(event *aggregate.BlockDataEvent) interface{}
}
