package dataaccess

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/kasuganosora/sqlexec/pkg/resource/domain"
)

// =============================================================================
// P0-1: Insert/Update/Delete bypass router, always use primary dataSource
// service.go lines 43, 84, 104 use s.dataSource directly instead of routing
// through s.router.Route(tableName). When routing is configured to a different
// data source, writes go to the wrong data source.
// =============================================================================

// TrackingDataSource records which operations were called on it
type TrackingDataSource struct {
	name        string
	insertCount int64
	updateCount int64
	deleteCount int64
}

func (t *TrackingDataSource) Connect(ctx context.Context) error { return nil }
func (t *TrackingDataSource) Close(ctx context.Context) error   { return nil }
func (t *TrackingDataSource) IsConnected() bool                 { return true }
func (t *TrackingDataSource) IsWritable() bool                  { return true }
func (t *TrackingDataSource) GetConfig() *domain.DataSourceConfig {
	return &domain.DataSourceConfig{Type: domain.DataSourceTypeMemory}
}
func (t *TrackingDataSource) GetTables(ctx context.Context) ([]string, error) {
	return []string{"orders"}, nil
}
func (t *TrackingDataSource) Query(ctx context.Context, tableName string, options *domain.QueryOptions) (*domain.QueryResult, error) {
	return &domain.QueryResult{
		Columns: []domain.ColumnInfo{{Name: "id", Type: "int"}},
		Rows:    []domain.Row{{"id": 1}},
	}, nil
}
func (t *TrackingDataSource) Insert(ctx context.Context, tableName string, rows []domain.Row, options *domain.InsertOptions) (int64, error) {
	atomic.AddInt64(&t.insertCount, 1)
	return 1, nil
}
func (t *TrackingDataSource) Update(ctx context.Context, tableName string, filters []domain.Filter, updates domain.Row, options *domain.UpdateOptions) (int64, error) {
	atomic.AddInt64(&t.updateCount, 1)
	return 1, nil
}
func (t *TrackingDataSource) Delete(ctx context.Context, tableName string, filters []domain.Filter, options *domain.DeleteOptions) (int64, error) {
	atomic.AddInt64(&t.deleteCount, 1)
	return 1, nil
}
func (t *TrackingDataSource) CreateTable(ctx context.Context, info *domain.TableInfo) error {
	return nil
}
func (t *TrackingDataSource) DropTable(ctx context.Context, tableName string) error     { return nil }
func (t *TrackingDataSource) TruncateTable(ctx context.Context, tableName string) error { return nil }
func (t *TrackingDataSource) GetTableInfo(ctx context.Context, tableName string) (*domain.TableInfo, error) {
	return &domain.TableInfo{
		Name:    tableName,
		Columns: []domain.ColumnInfo{{Name: "id", Type: "int"}},
	}, nil
}
func (t *TrackingDataSource) Execute(ctx context.Context, sql string) (*domain.QueryResult, error) {
	return nil, nil
}

func TestInsertShouldRouteToCorrectDataSource(t *testing.T) {
	primaryDS := &TrackingDataSource{name: "primary"}
	routedDS := &TrackingDataSource{name: "routed"}

	service := NewDataService(primaryDS).(*DataService)

	// Register and route "orders" table to the routed data source
	service.manager.RegisterDataSource("orders_ds", routedDS)
	service.router.AddRoute("orders", "orders_ds")

	ctx := context.Background()
	data := map[string]interface{}{"id": 1, "item": "widget"}

	_, err := service.Insert(ctx, "orders", data)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Insert should go to routedDS, not primaryDS
	if atomic.LoadInt64(&routedDS.insertCount) == 0 {
		t.Errorf("Insert did NOT route to the correct data source (routed), went to primary instead")
	}
	if atomic.LoadInt64(&primaryDS.insertCount) > 0 {
		t.Errorf("Insert incorrectly went to primary data source instead of routed data source")
	}
}

func TestUpdateShouldRouteToCorrectDataSource(t *testing.T) {
	primaryDS := &TrackingDataSource{name: "primary"}
	routedDS := &TrackingDataSource{name: "routed"}

	service := NewDataService(primaryDS).(*DataService)
	service.manager.RegisterDataSource("orders_ds", routedDS)
	service.router.AddRoute("orders", "orders_ds")

	ctx := context.Background()
	data := map[string]interface{}{"item": "updated_widget"}
	where := &domain.Filter{Field: "id", Operator: "=", Value: 1}

	err := service.Update(ctx, "orders", data, where)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Update should go to routedDS, not primaryDS
	if atomic.LoadInt64(&routedDS.updateCount) == 0 {
		t.Errorf("Update did NOT route to the correct data source (routed), went to primary instead")
	}
	if atomic.LoadInt64(&primaryDS.updateCount) > 0 {
		t.Errorf("Update incorrectly went to primary data source instead of routed data source")
	}
}

func TestDeleteShouldRouteToCorrectDataSource(t *testing.T) {
	primaryDS := &TrackingDataSource{name: "primary"}
	routedDS := &TrackingDataSource{name: "routed"}

	service := NewDataService(primaryDS).(*DataService)
	service.manager.RegisterDataSource("orders_ds", routedDS)
	service.router.AddRoute("orders", "orders_ds")

	ctx := context.Background()
	where := &domain.Filter{Field: "id", Operator: "=", Value: 1}

	err := service.Delete(ctx, "orders", where)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Delete should go to routedDS, not primaryDS
	if atomic.LoadInt64(&routedDS.deleteCount) == 0 {
		t.Errorf("Delete did NOT route to the correct data source (routed), went to primary instead")
	}
	if atomic.LoadInt64(&primaryDS.deleteCount) > 0 {
		t.Errorf("Delete incorrectly went to primary data source instead of routed data source")
	}
}
