package db

import (
	"context"
	"path"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
)

type DB struct {
	driver *ydb.Driver
	logger *zap.Logger
}

func NewDB(driver *ydb.Driver, logger *zap.Logger) *DB {
	return &DB{driver: driver, logger: logger}
}

func (d *DB) tablePath(tableName string) string {
	return path.Join(d.driver.Name(), tableName)
}

func (d *DB) Close(ctx context.Context) error {
	return d.driver.Close(ctx)
}

func (d *DB) CreateTable(ctx context.Context, tableName string, opts ...options.CreateTableOption) error {
	return d.driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.CreateTable(ctx, d.tablePath(tableName), opts...)
	})
}

func (d *DB) BulkUpsert(ctx context.Context, tableName string, rows []types.Value) (err error) {
	return retry.Retry(ctx, func(ctx context.Context) (err error) {
		return d.driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
			start := time.Now()
			defer func() {
				if err == nil {
					d.logger.Debug("rows inserted", zap.Int("records", len(rows)),
						zap.Duration("cost", time.Since(start)))
				}
			}()

			return s.BulkUpsert(ctx, d.tablePath(tableName), types.ListValue(rows...))
		})
	}, retry.WithIdempotent(true))
}
