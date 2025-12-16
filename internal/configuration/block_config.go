package configuration

// BlockConfig holds configuration for block processing
type BlockConfig struct {
	BatchDataMaxRowCnt        int64 `mapstructure:"batchDataMaxRowCnt"`
	BatchDataMaxByteSize      int64 `mapstructure:"batchDataMaxByteSize"`
	ByteBufferFixedCacheSize  int   `mapstructure:"byteBufferFixedCacheSize"`
	ByteBufferDynamicCacheSize int  `mapstructure:"byteBufferDynamicCacheSize"`
	BlockSize                 int   `mapstructure:"blockSize"`
}

// NewBlockConfig creates a new BlockConfig with default values
func NewBlockConfig() *BlockConfig {
	return &BlockConfig{
		BatchDataMaxRowCnt:        10000,
		BatchDataMaxByteSize:      10485760, // 10MB
		ByteBufferFixedCacheSize:  100,
		ByteBufferDynamicCacheSize: 50,
		BlockSize:                 8192,
	}
}

// GetBatchDataMaxRowCnt returns the maximum number of rows per batch
func (bc *BlockConfig) GetBatchDataMaxRowCnt() int64 {
	return bc.BatchDataMaxRowCnt
}

// GetBatchDataMaxByteSize returns the maximum byte size per batch
func (bc *BlockConfig) GetBatchDataMaxByteSize() int64 {
	return bc.BatchDataMaxByteSize
}

// GetByteBufferFixedCacheSize returns the fixed cache size
func (bc *BlockConfig) GetByteBufferFixedCacheSize() int {
	return bc.ByteBufferFixedCacheSize
}

// GetByteBufferDynamicCacheSize returns the dynamic cache size
func (bc *BlockConfig) GetByteBufferDynamicCacheSize() int {
	return bc.ByteBufferDynamicCacheSize
}

// GetBlockSize returns the block size
func (bc *BlockConfig) GetBlockSize() int {
	return bc.BlockSize
}