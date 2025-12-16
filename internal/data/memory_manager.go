// memory_manager.go 是Berry Rapids Go项目中内存管理模块的核心实现文件。
// 该文件定义了MemoryManager 结构体，实现了高效的内存分配和回收机制。
// 设计原理：通过对象池(sync.Pool)减少GC压力，提供固定大小和动态大小的内存分配策略
// 实现方式：使用sync.Pool复用字节切片，通过互斥锁保护共享状态，跟踪内存使用统计信息
package data

import (
	"sync"
	"go.uber.org/zap"
)

// MemoryManager 负责批处理过程中的内存分配和释放
// 设计说明：采用对象池模式减少内存分配开销，提供两种内存分配策略：
// 1. 固定大小内存分配：适用于已知固定大小的场景
// 2. 动态大小内存分配：适用于大小可变的场景
type MemoryManager struct {
	logger            *zap.Logger // 日志记录器
	fixedCacheSize    int         // 固定缓存大小
	dynamicCacheSize  int         // 动态缓存大小
	blockSize         int         // 块大小，用于固定大小分配
	fixedPool         sync.Pool   // 固定大小内存池
	dynamicPool       sync.Pool   // 动态大小内存池
	allocatedBytes    int64       // 已分配字节数
	peakMemoryUsage   int64       // 峰值内存使用量
	mutex             sync.Mutex  // 互斥锁，保护共享状态
}

// NewMemoryManager 创建新的内存管理器实例
// 设计原理：初始化内存管理器配置和对象池，预分配内存以提高性能
// 参数说明：
//   logger - 日志记录器
//   fixedCacheSize - 固定缓存大小
//   dynamicCacheSize - 动态缓存大小
//   blockSize - 块大小，用于固定大小分配
// 返回值：MemoryManager实例
func NewMemoryManager(
	logger *zap.Logger,
	fixedCacheSize, dynamicCacheSize, blockSize int,
) *MemoryManager {
	// 创建内存管理器实例
	mm := &MemoryManager{
		logger:           logger,
		fixedCacheSize:   fixedCacheSize,
		dynamicCacheSize: dynamicCacheSize,
		blockSize:        blockSize,
	}

	// 初始化固定大小内存池
	// 当池中无可用对象时，创建指定大小的字节切片
	mm.fixedPool.New = func() interface{} {
		return make([]byte, blockSize)
	}

	// 初始化动态大小内存池
	// 当池中无可用对象时，创建初始长度为0、容量为blockSize的字节切片
	mm.dynamicPool.New = func() interface{} {
		return make([]byte, 0, blockSize)
	}

	// 返回内存管理器实例
	return mm
}

// AllocateFixed 从池中分配固定大小的字节切片
// 设计原理：优先从对象池获取已存在的内存块，减少内存分配开销
// 实现方式：使用sync.Pool的Get方法获取内存块，并更新内存使用统计
// 返回值：固定大小的字节切片
func (mm *MemoryManager) AllocateFixed() []byte {
	// 从固定大小内存池获取字节切片
	buffer := mm.fixedPool.Get().([]byte)
	
	// 更新内存使用统计信息
	mm.mutex.Lock()
	// 增加已分配字节数
	mm.allocatedBytes += int64(len(buffer))
	// 更新峰值内存使用量
	if mm.allocatedBytes > mm.peakMemoryUsage {
		mm.peakMemoryUsage = mm.allocatedBytes
	}
	mm.mutex.Unlock()
	
	// 记录分配日志
	mm.logger.Debug("Allocated fixed buffer", zap.Int("size", len(buffer)))
	// 返回分配的字节切片
	return buffer
}

// ReleaseFixed 将固定大小的字节切片归还到池中
// 设计原理：重置字节切片内容并放回对象池，供后续重复使用
// 实现方式：清零字节切片内容防止数据泄露，使用sync.Pool的Put方法归还内存块
// 参数说明：buffer - 需要释放的字节切片
func (mm *MemoryManager) ReleaseFixed(buffer []byte) {
	// 重置缓冲区内容以防止数据泄露
	for i := range buffer {
		buffer[i] = 0
	}
	
	// 将字节切片放回固定大小内存池
	mm.fixedPool.Put(buffer)
	
	// 更新内存使用统计信息
	mm.mutex.Lock()
	// 减少已分配字节数
	mm.allocatedBytes -= int64(len(buffer))
	mm.mutex.Unlock()
	
	// 记录释放日志
	mm.logger.Debug("Released fixed buffer", zap.Int("size", len(buffer)))
}

// AllocateDynamic 从池中分配动态大小的字节切片
// 设计原理：根据请求大小动态分配内存，优先复用现有内存块
// 实现方式：从动态内存池获取内存块，如果容量不足则创建新内存块
// 参数说明：size - 请求的字节切片大小
// 返回值：指定大小的字节切片
func (mm *MemoryManager) AllocateDynamic(size int) []byte {
	// 从动态大小内存池获取字节切片
	buffer := mm.dynamicPool.Get().([]byte)
	
	// 确保缓冲区具有足够的容量
	if cap(buffer) < size {
		// 如果容量不足，创建新的字节切片
		buffer = make([]byte, size)
	} else {
		// 如果容量足够，调整长度为所需大小
		buffer = buffer[:size]
	}
	
	// 更新内存使用统计信息
	mm.mutex.Lock()
	// 增加已分配字节数
	mm.allocatedBytes += int64(len(buffer))
	// 更新峰值内存使用量
	if mm.allocatedBytes > mm.peakMemoryUsage {
		mm.peakMemoryUsage = mm.allocatedBytes
	}
	mm.mutex.Unlock()
	
	// 记录分配日志
	mm.logger.Debug("Allocated dynamic buffer", zap.Int("size", len(buffer)))
	// 返回分配的字节切片
	return buffer
}

// ReleaseDynamic 将动态大小的字节切片归还到池中
// 设计原理：重置字节切片内容并放回对象池，保持容量以便后续复用
// 实现方式：清零字节切片内容防止数据泄露，重置长度但保持容量，使用sync.Pool的Put方法归还内存块
// 参数说明：buffer - 需要释放的字节切片
func (mm *MemoryManager) ReleaseDynamic(buffer []byte) {
	// 重置缓冲区内容以防止数据泄露
	for i := range buffer {
		buffer[i] = 0
	}
	
	// 重置长度但保持容量，以便后续复用
	buffer = buffer[:0]
	// 将字节切片放回动态大小内存池
	mm.dynamicPool.Put(buffer)
	
	// 更新内存使用统计信息
	mm.mutex.Lock()
	// 减少已分配字节数
	mm.allocatedBytes -= int64(len(buffer))
	mm.mutex.Unlock()
	
	// 记录释放日志
	mm.logger.Debug("Released dynamic buffer", zap.Int("size", len(buffer)))
}

// GetAllocatedBytes 返回当前已分配的字节数
// 设计原理：提供实时内存使用情况查询接口
// 返回值：当前已分配的字节数
func (mm *MemoryManager) GetAllocatedBytes() int64 {
	// 加锁保护共享状态
	mm.mutex.Lock()
	// 使用defer确保在函数返回前解锁
	defer mm.mutex.Unlock()
	// 返回当前已分配的字节数
	return mm.allocatedBytes
}

// GetPeakMemoryUsage 返回峰值内存使用量
// 设计原理：提供历史最高内存使用量查询接口，用于性能分析和调优
// 返回值：峰值内存使用量
func (mm *MemoryManager) GetPeakMemoryUsage() int64 {
	// 加锁保护共享状态
	mm.mutex.Lock()
	// 使用defer确保在函数返回前解锁
	defer mm.mutex.Unlock()
	// 返回峰值内存使用量
	return mm.peakMemoryUsage
}

// GetStats 返回内存使用统计信息
// 设计原理：提供统一的内存统计信息查询接口
// 返回值：包含已分配字节数和峰值内存使用量的映射
func (mm *MemoryManager) GetStats() map[string]int64 {
	// 加锁保护共享状态
	mm.mutex.Lock()
	// 使用defer确保在函数返回前解锁
	defer mm.mutex.Unlock()
	
	// 返回内存使用统计信息
	return map[string]int64{
		"allocatedBytes":   mm.allocatedBytes,   // 当前已分配字节数
		"peakMemoryUsage":  mm.peakMemoryUsage,  // 峰值内存使用量
	}
}