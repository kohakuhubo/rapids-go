package model

// BatchData represents a batch of data to be processed
type BatchData struct {
	StartRowID      int64  `json:"startRowId"`
	EndRowID        int64  `json:"endRowId"`
	DataSourceID    string `json:"dataSourceId"`
	Status          string `json:"status"`
	DataStartTime   int64  `json:"dataStartTime"`
	DataEndTime     int64  `json:"dataEndTime"`
	Content         []byte `json:"content"`
	SizeInBytes     int64  `json:"sizeInBytes"`
	NumberOfRows    int64  `json:"numberOfRows"`
	AggregateTypes  []string `json:"aggregateTypes"`
}

// NewBatchData creates a new BatchData instance
func NewBatchData(startRowID, endRowID int64, dataSourceID string) *BatchData {
	return &BatchData{
		StartRowID:   startRowID,
		EndRowID:     endRowID,
		DataSourceID: dataSourceID,
		Status:       "PENDING",
		AggregateTypes: []string{},
	}
}

// SetContent sets the content and updates size
func (bd *BatchData) SetContent(content []byte) {
	bd.Content = content
	bd.SizeInBytes = int64(len(content))
}

// SetTimeRange sets the data time range
func (bd *BatchData) SetTimeRange(startTime, endTime int64) {
	bd.DataStartTime = startTime
	bd.DataEndTime = endTime
}

// SetStatus updates the batch status
func (bd *BatchData) SetStatus(status string) {
	bd.Status = status
}

// AddAggregateType adds an aggregate type to the batch
func (bd *BatchData) AddAggregateType(aggType string) {
	for _, t := range bd.AggregateTypes {
		if t == aggType {
			return
		}
	}
	bd.AggregateTypes = append(bd.AggregateTypes, aggType)
}

// IsEmpty checks if the batch is empty
func (bd *BatchData) IsEmpty() bool {
	return bd.Content == nil || len(bd.Content) == 0
}

// IsFull checks if the batch has reached its limits
func (bd *BatchData) IsFull(maxRows, maxSize int64) bool {
	return bd.NumberOfRows >= maxRows || bd.SizeInBytes >= maxSize
}