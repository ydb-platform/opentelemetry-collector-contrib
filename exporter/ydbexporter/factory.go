package ydbexporter

import (
	"context"
	"fmt"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/logs"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/metrics"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/traces"
)

// NewFactory creates a factory for YDB exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		config.DefaultConfig,
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithMetrics(createMetricExporter, metadata.MetricsStability),
	)
}

// createLogsExporter creates a new exporter for logs.
// Logs are directly insert into YDB.
func createLogsExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	cfg component.Config,
) (exporter.Logs, error) {
	c := cfg.(*config.Config)
	exporter, err := logs.NewExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure ydb logs exporter: %w", err)
	}

	return exporterhelper.NewLogsExporter(
		ctx,
		set,
		cfg,
		exporter.PushData,
		exporterhelper.WithStart(exporter.Start),
		exporterhelper.WithShutdown(exporter.Shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
	)
}

// createTracesExporter creates a new exporter for traces.
// Traces are directly insert into YDB.
func createTracesExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	cfg component.Config,
) (exporter.Traces, error) {
	c := cfg.(*config.Config)
	exporter, err := traces.NewExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure ydb logs exporter: %w", err)
	}

	return exporterhelper.NewTracesExporter(
		ctx,
		set,
		cfg,
		exporter.PushData,
		exporterhelper.WithStart(exporter.Start),
		exporterhelper.WithShutdown(exporter.Shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
	)
}

// createMetricsExporter creates a new exporter for metrics.
// Metrics are directly insert into YDB.
func createMetricExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	cfg component.Config,
) (exporter.Metrics, error) {
	c := cfg.(*config.Config)
	exporter, err := metrics.NewExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure ydb logs exporter: %w", err)
	}

	return exporterhelper.NewMetricsExporter(
		ctx,
		set,
		cfg,
		exporter.PushData,
		exporterhelper.WithStart(exporter.Start),
		exporterhelper.WithShutdown(exporter.Shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
	)
}
