package source

// SourceEntry 表示来自数据源的单个条目
type SourceEntry interface {
	GetValue() []byte
	GetRowId() int64
}

// Source 表示数据源
type Source interface {
	Next() (SourceEntry, error)
	Close() error
}
