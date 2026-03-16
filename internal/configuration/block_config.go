package configuration

// BlockConfig 保存块处理配置
type BlockConfig struct {
	BatchDataMaxRowCnt         int64 `mapstructure:"batchDataMaxRowCnt"`
	BatchDataMaxByteSize       int64 `mapstructure:"batchDataMaxByteSize"`
	ByteBufferFixedCacheSize   int   `mapstructure:"byteBufferFixedCacheSize"`
	ByteBufferDynamicCacheSize int   `mapstructure:"byteBufferDynamicCacheSize"`
	BlockSize                  int   `mapstructure:"blockSize"`
}

// NewBlockConfig 创建一个带有默认值的新 BlockConfig
func NewBlockConfig() *BlockConfig {
	return &BlockConfig{
		BatchDataMaxRowCnt:         10000,
		BatchDataMaxByteSize:       10485760, // 10MB
		ByteBufferFixedCacheSize:   100,
		ByteBufferDynamicCacheSize: 50,
		BlockSize:                  8192,
	}
}

// GetBatchDataMaxRowCnt 返回每批的最大行数
func (bc *BlockConfig) GetBatchDataMaxRowCnt() int64 {
	return bc.BatchDataMaxRowCnt
}

// GetBatchDataMaxByteSize 返回每批的最大字节数
func (bc *BlockConfig) GetBatchDataMaxByteSize() int64 {
	return bc.BatchDataMaxByteSize
}

// GetByteBufferFixedCacheSize 返回固定缓存大小
func (bc *BlockConfig) GetByteBufferFixedCacheSize() int {
	return bc.ByteBufferFixedCacheSize
}

// GetByteBufferDynamicCacheSize 返回动态缓存大小
func (bc *BlockConfig) GetByteBufferDynamicCacheSize() int {
	return bc.ByteBufferDynamicCacheSize
}

// GetBlockSize 返回块大小
func (bc *BlockConfig) GetBlockSize() int {
	return bc.BlockSize
}
