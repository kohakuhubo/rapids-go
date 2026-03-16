package model

import "fmt"

// FieldBatch 封装批量字段数据，提供字节大小和行数统计
// 类似于 Java 的 ByteBuffer，专门用于存储列式数据
type FieldBatch struct {
	data        map[string][][]byte // 字段名到字节数组切片的映射
	rowCount    int                 // 行数（所有字段应该有相同的行数）
	size        int64               // 总字节大小
	initialized bool                // 是否已初始化统计信息
}

// NewFieldBatch 创建新的 FieldBatch
func NewFieldBatch(data map[string][][]byte) *FieldBatch {
	return &FieldBatch{
		data:     data,
		rowCount: -1, // -1 表示未计算
		size:     -1, // -1 表示未计算
	}
}

// GetRowCount 获取行数
// 如果所有字段的切片长度一致，返回该长度；否则返回 -1
func (fb *FieldBatch) GetRowCount() int {
	if fb.initialized {
		return fb.rowCount
	}
	fb.calculateStats()
	return fb.rowCount
}

// GetTotalSize 获取总字节大小
// 计算所有字段数据的总字节数（不包括 nil 值）
func (fb *FieldBatch) GetTotalSize() int64 {
	if fb.initialized {
		return fb.size
	}
	fb.calculateStats()
	return fb.size
}

// GetFieldSize 获取指定字段的总字节大小
func (fb *FieldBatch) GetFieldSize(fieldName string) int64 {
	var total int64
	if values, exists := fb.data[fieldName]; exists {
		for _, v := range values {
			if v != nil {
				total += int64(len(v))
			}
		}
	}
	return total
}

// GetFieldRowCount 获取指定字段的有效行数（非 nil 的行数）
func (fb *FieldBatch) GetFieldRowCount(fieldName string) int {
	if values, exists := fb.data[fieldName]; exists {
		count := 0
		for _, v := range values {
			if v != nil {
				count++
			}
		}
		return count
	}
	return 0
}

// GetData 获取底层数据
func (fb *FieldBatch) GetData() map[string][][]byte {
	return fb.data
}

// GetField 获取指定字段的数据
func (fb *FieldBatch) GetField(fieldName string) [][]byte {
	return fb.data[fieldName]
}

// calculateStats 计算统计信息（行数和总大小）
func (fb *FieldBatch) calculateStats() {
	if len(fb.data) == 0 {
		fb.rowCount = 0
		fb.size = 0
		fb.initialized = true
		return
	}

	// 计算行数（以第一个字段的长度为基准）
	firstField := ""
	for fieldName := range fb.data {
		firstField = fieldName
		break
	}

	if firstField != "" {
		fb.rowCount = len(fb.data[firstField])

		// 验证所有字段的行数是否一致
		for _, values := range fb.data {
			if len(values) != fb.rowCount {
				fb.rowCount = -1 // 行数不一致
				break
			}
		}
	}

	// 计算总字节大小
	fb.size = 0
	for _, values := range fb.data {
		for _, v := range values {
			if v != nil {
				fb.size += int64(len(v))
			}
		}
	}

	fb.initialized = true
}

// GetFieldNames 获取所有字段名
func (fb *FieldBatch) GetFieldNames() []string {
	names := make([]string, 0, len(fb.data))
	for name := range fb.data {
		names = append(names, name)
	}
	return names
}

// HasField 检查是否包含指定字段
func (fb *FieldBatch) HasField(fieldName string) bool {
	_, exists := fb.data[fieldName]
	return exists
}

// GetStats 获取详细的统计信息
func (fb *FieldBatch) GetStats() map[string]interface{} {
	if !fb.initialized {
		fb.calculateStats()
	}

	stats := map[string]interface{}{
		"rowCount":       fb.rowCount,
		"totalSize":      fb.size,
		"fieldCount":     len(fb.data),
		"fieldSizes":     make(map[string]int64),
		"fieldRowCounts": make(map[string]int),
	}

	for fieldName := range fb.data {
		stats["fieldSizes"].(map[string]int64)[fieldName] = fb.GetFieldSize(fieldName)
		stats["fieldRowCounts"].(map[string]int)[fieldName] = fb.GetFieldRowCount(fieldName)
	}

	return stats
}

// Clone 创建 FieldBatch 的浅拷贝（不拷贝底层数据）
func (fb *FieldBatch) Clone() *FieldBatch {
	newData := make(map[string][][]byte, len(fb.data))
	for k, v := range fb.data {
		newData[k] = v
	}

	return &FieldBatch{
		data:        newData,
		rowCount:    fb.rowCount,
		size:        fb.size,
		initialized: fb.initialized,
	}
}

// FilterFields 过滤字段，只保留指定的字段
func (fb *FieldBatch) FilterFields(fieldNames []string) *FieldBatch {
	nameSet := make(map[string]bool)
	for _, name := range fieldNames {
		nameSet[name] = true
	}

	filteredData := make(map[string][][]byte)
	for fieldName, values := range fb.data {
		if nameSet[fieldName] {
			filteredData[fieldName] = values
		}
	}

	return NewFieldBatch(filteredData)
}

// Merge 合并另一个 FieldBatch
// 注意：这会假设两个批次有相同的行数
func (fb *FieldBatch) Merge(other *FieldBatch) error {
	for fieldName, values := range other.data {
		if _, exists := fb.data[fieldName]; exists {
			// 字段已存在，可以选择覆盖或合并（这里选择覆盖）
			fb.data[fieldName] = values
		} else {
			// 新字段，直接添加
			fb.data[fieldName] = values
		}
	}

	// 重新计算统计信息
	fb.initialized = false
	return nil
}

// String 返回 FieldBatch 的字符串表示
func (fb *FieldBatch) String() string {
	if !fb.initialized {
		fb.calculateStats()
	}

	return fmt.Sprintf("FieldBatch{rowCount: %d, fieldCount: %d, totalSize: %d bytes}",
		fb.rowCount, len(fb.data), fb.size)
}
