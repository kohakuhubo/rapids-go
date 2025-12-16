package persistece

import (
	"context"
	"berry-rapids-go/internal/model"
	"berry-rapids-go/internal/configuration"
	"berry-rapids-go/internal/clickhouse/client"
	"go.uber.org/zap"
)

// PersistenceServer manages multiple persistence handlers
type PersistenceServer struct {
	logger            *zap.Logger
	config            *configuration.BlockConfig
	clickhouseClient  *client.EnhancedClickHouseClient
	handlers          map[string]*PersistenceHandler
	defaultHandler    *PersistenceHandler
}

// NewPersistenceServer creates a new PersistenceServer
func NewPersistenceServer(
	logger *zap.Logger,
	config *configuration.BlockConfig,
	clickhouseClient *client.EnhancedClickHouseClient,
) *PersistenceServer {
	return &PersistenceServer{
		logger:           logger,
		config:           config,
		clickhouseClient: clickhouseClient,
		handlers:         make(map[string]*PersistenceHandler),
	}
}

// RegisterHandler registers a persistence handler for a specific data source
func (ps *PersistenceServer) RegisterHandler(dataSourceID, tableName string) {
	handler := NewPersistenceHandler(
		ps.logger,
		ps.config,
		ps.clickhouseClient,
		tableName,
	)
	
	ps.handlers[dataSourceID] = handler
	
	// Set as default handler if it's the first one
	if ps.defaultHandler == nil {
		ps.defaultHandler = handler
	}
	
	ps.logger.Info("Registered persistence handler",
		zap.String("dataSourceID", dataSourceID),
		zap.String("tableName", tableName))
}

// Handle processes a batch of data using the appropriate handler
func (ps *PersistenceServer) Handle(ctx context.Context, batch *model.BatchData) error {
	if batch == nil {
		return nil
	}

	// Get the appropriate handler for this data source
	handler, exists := ps.handlers[batch.DataSourceID]
	if !exists {
		// Use default handler if available
		if ps.defaultHandler != nil {
			handler = ps.defaultHandler
			ps.logger.Debug("Using default handler for data source",
				zap.String("dataSourceID", batch.DataSourceID))
		} else {
			return nil
		}
	}

	return handler.Handle(ctx, batch)
}

// GetHandler returns the persistence handler for a specific data source
func (ps *PersistenceServer) GetHandler(dataSourceID string) (*PersistenceHandler, bool) {
	handler, exists := ps.handlers[dataSourceID]
	return handler, exists
}

// GetStats returns statistics for all handlers
func (ps *PersistenceServer) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})
	
	for dataSourceID, handler := range ps.handlers {
		stats[dataSourceID] = handler.GetStats()
	}
	
	return stats
}