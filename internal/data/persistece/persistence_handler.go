package persistece

import (
	"context"
	"berry-rapids-go/internal/model"
	"berry-rapids-go/internal/clickhouse/client"
	"berry-rapids-go/internal/configuration"
	"go.uber.org/zap"
)

// PersistenceHandler handles persistence of batch data
type PersistenceHandler struct {
	logger         *zap.Logger
	config         *configuration.BlockConfig
	clickhouseClient *client.EnhancedClickHouseClient
	tableName      string
	maxRetries     int
}

// NewPersistenceHandler creates a new PersistenceHandler
func NewPersistenceHandler(
	logger *zap.Logger,
	config *configuration.BlockConfig,
	clickhouseClient *client.EnhancedClickHouseClient,
	tableName string,
) *PersistenceHandler {
	return &PersistenceHandler{
		logger:           logger,
		config:           config,
		clickhouseClient: clickhouseClient,
		tableName:        tableName,
		maxRetries:       3,
	}
}

// Handle processes a batch of data and persists it to ClickHouse
func (ph *PersistenceHandler) Handle(ctx context.Context, batch *model.BatchData) error {
	if batch == nil || batch.IsEmpty() {
		ph.logger.Debug("Skipping empty batch")
		return nil
	}

	ph.logger.Info("Handling batch for persistence",
		zap.Int64("startRowID", batch.StartRowID),
		zap.Int64("endRowID", batch.EndRowID),
		zap.Int64("sizeInBytes", batch.SizeInBytes),
		zap.Int64("numberOfRows", batch.NumberOfRows))

	// Insert batch into ClickHouse with retry logic
	if err := ph.clickhouseClient.InsertBatchWithRetry(ctx, ph.tableName, batch, ph.maxRetries); err != nil {
		ph.logger.Error("Failed to persist batch",
			zap.Int64("startRowID", batch.StartRowID),
			zap.Int64("endRowID", batch.EndRowID),
			zap.Error(err))
		return err
	}

	ph.logger.Info("Successfully persisted batch",
		zap.Int64("startRowID", batch.StartRowID),
		zap.Int64("endRowID", batch.EndRowID),
		zap.Int64("sizeInBytes", batch.SizeInBytes),
		zap.Int64("numberOfRows", batch.NumberOfRows))

	// Update batch status
	batch.SetStatus("PERSISTED")

	return nil
}

// SetMaxRetries sets the maximum number of retries for persistence operations
func (ph *PersistenceHandler) SetMaxRetries(maxRetries int) {
	ph.maxRetries = maxRetries
}

// GetStats returns persistence statistics
func (ph *PersistenceHandler) GetStats() map[string]interface{} {
	// In a real implementation, you would track statistics like:
	// - Number of batches processed
	// - Number of successful/failed insertions
	// - Average insertion time
	// - etc.
	
	return map[string]interface{}{
		"maxRetries": ph.maxRetries,
		"tableName":  ph.tableName,
	}
}