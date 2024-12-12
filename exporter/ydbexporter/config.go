package ydbexporter

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"
	_ "github.com/ydb-platform/ydb-go-sdk/v3"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	defaultEndpoint         = "grpcs://localhost:2135"
	defaultAuthType         = "anonymous"
	defaultDatabase         = "/local"
	defaultLogsTableName    = "otel_logs"
	defaultMetricsTableName = "otel_metrics"
	defaultTracesTableName  = "otel_traces"
	defaultPartitionsCount  = 64
	defaultTTL              = 0
)

func WithDefaultConfig(fns ...func(*config.Config)) *config.Config {
	cfg := DefaultConfig().(*config.Config)
	for _, fn := range fns {
		fn(cfg)
	}
	return cfg
}

func DefaultConfig() component.Config {
	queueSettings := exporterhelper.NewDefaultQueueSettings()
	queueSettings.NumConsumers = 1

	return &config.Config{
		TimeoutSettings:  exporterhelper.NewDefaultTimeoutSettings(),
		QueueSettings:    queueSettings,
		ConnectionParams: map[string]string{},
		AuthType:         defaultAuthType,
		Endpoint:         defaultEndpoint,
		Database:         defaultDatabase,
		MetricsTable: config.TableConfig{
			Name:            defaultMetricsTableName,
			TTL:             defaultTTL,
			PartitionsCount: defaultPartitionsCount,
		},
		LogsTable: config.TableConfig{
			Name:            defaultLogsTableName,
			TTL:             defaultTTL,
			PartitionsCount: defaultPartitionsCount,
		},
		TracesTable: config.TableConfig{
			Name:            defaultTracesTableName,
			TTL:             defaultTTL,
			PartitionsCount: defaultPartitionsCount,
		},
	}
}
