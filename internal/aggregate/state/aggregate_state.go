package aggregate

// AggregateState 聚合状态结构体
type AggregateState struct {
	StartRowID    int64 `json:"startRowId"`
	EndRowID      int64 `json:"endRowId"`
	DataStartTime int64 `json:"dataStartTime"`
	DataEndTime   int64 `json:"dataEndTime"`
}

// NewAggregateState 创建新的聚合状态
func NewAggregateState(startRowID, endRowID, dataStartTime, dataEndTime int64) *AggregateState {
	return &AggregateState{
		StartRowID:    startRowID,
		EndRowID:      endRowID,
		DataStartTime: dataStartTime,
		DataEndTime:   dataEndTime,
	}
}

// GetStartRowID 获取起始行ID
func (s *AggregateState) GetStartRowID() int64 {
	return s.StartRowID
}

// GetEndRowID 获取结束行ID
func (s *AggregateState) GetEndRowID() int64 {
	return s.EndRowID
}

// GetDataStartTime 获取数据开始时间
func (s *AggregateState) GetDataStartTime() int64 {
	return s.DataStartTime
}

// GetDataEndTime 获取数据结束时间
func (s *AggregateState) GetDataEndTime() int64 {
	return s.DataEndTime
}