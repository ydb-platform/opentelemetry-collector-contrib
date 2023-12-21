package metrics

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/db"
)

var (
	supportedExporters = map[pmetric.MetricType]exporter{
		pmetric.MetricTypeGauge: &gauge{},
		pmetric.MetricTypeSum:   &sum{},
	}
)

type exporter interface {
	createTableOptions(config *config.TableConfig) []options.CreateTableOption
	tableName(*config.TableConfig) string
	createRecords(resourceMetrics pmetric.ResourceMetrics, scopeMetrics pmetric.ScopeMetrics, metric pmetric.Metric) ([]types.Value, error)
}

type Exporter struct {
	client *db.DB
	logger *zap.Logger
	cfg    *config.TableConfig
}

func NewExporter(logger *zap.Logger, cfg *config.Config) (*Exporter, error) {
	client, err := db.NewFactory(cfg, logger).BuildDB()
	if err != nil {
		return nil, err
	}
	return &Exporter{
		client: client,
		logger: logger,
		cfg:    &cfg.MetricsTable,
	}, nil
}

func (e *Exporter) Start(ctx context.Context, _ component.Host) error {
	err := e.createTable(ctx)
	if err != nil {
		return err
	}
	e.logger.Debug("YDB metrics exporter started")
	return nil
}

func (e *Exporter) Shutdown(ctx context.Context) error {
	defer e.logger.Debug("YDB metrics exporter stopped")
	if e.client != nil {
		return e.client.Close(ctx)
	}
	return nil
}

func (e *Exporter) createTable(ctx context.Context) error {
	for _, exporter := range supportedExporters {
		opts := exporter.createTableOptions(e.cfg)
		err := e.client.CreateTable(ctx, exporter.tableName(e.cfg), opts...)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Exporter) PushData(ctx context.Context, md pmetric.Metrics) error {
	values := make(map[pmetric.MetricType][]types.Value)
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		resourceMetrics := md.ResourceMetrics().At(i)
		for j := 0; j < resourceMetrics.ScopeMetrics().Len(); j++ {
			scopeMetrics := resourceMetrics.ScopeMetrics().At(j)
			for k := 0; k < scopeMetrics.Metrics().Len(); k++ {
				metric := scopeMetrics.Metrics().At(k)
				records, err := e.createRecords(resourceMetrics, scopeMetrics, metric)
				if err != nil {
					return err
				}
				values[metric.Type()] = append(values[metric.Type()], records...)
			}
		}
	}

	return e.upsertValues(ctx, values)
}

func (e *Exporter) createRecords(resourceMetrics pmetric.ResourceMetrics, scopeMetrics pmetric.ScopeMetrics, metric pmetric.Metric) ([]types.Value, error) {
	exporter, ok := supportedExporters[metric.Type()]
	if !ok {
		return nil, fmt.Errorf("unsupported metrics type, %s", metric.Type().String())
	}
	return exporter.createRecords(resourceMetrics, scopeMetrics, metric)
}

func (e *Exporter) upsertValues(ctx context.Context, valuesMap map[pmetric.MetricType][]types.Value) error {
	errsChan := make(chan error, len(valuesMap))
	wg := &sync.WaitGroup{}
	for metricType, values := range valuesMap {
		wg.Add(1)
		go func(metricType pmetric.MetricType, values []types.Value) {
			defer wg.Done()
			exporter := supportedExporters[metricType]
			errsChan <- e.client.BulkUpsert(ctx, exporter.tableName(e.cfg), values)
		}(metricType, values)
	}
	wg.Wait()
	close(errsChan)

	var errs error
	for err := range errsChan {
		errs = errors.Join(errs, err)
	}
	return errs
}
