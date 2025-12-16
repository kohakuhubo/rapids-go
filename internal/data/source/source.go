package source

// SourceEntry represents a single entry from a data source
type SourceEntry interface {
	GetValue() []byte
	GetRowId() int64
}

// Source represents a data source
type Source interface {
	Next() (SourceEntry, error)
	Close() error
}
