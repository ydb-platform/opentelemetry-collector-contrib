package config

import (
	"time"

	_ "github.com/ydb-platform/ydb-go-sdk/v3"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"

	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type TableConfig struct {
	// TTL is The data time-to-live example 30m, 48h. 0 means no ttl.
	TTL time.Duration `mapstructure:"ttl"`
	// Name is the table name
	Name            string `mapstructure:"name"`
	PartitionsCount uint64 `mapstructure:"partitions_count"`
}

// Config defines configuration for YDB exporter.
type Config struct {
	exporterhelper.TimeoutSettings `mapstructure:",squash"`
	exporterhelper.QueueSettings   `mapstructure:"sending_queue"`
	// AuthType set authentication method
	// one of "anonymous" | "serviceAccountKey" | "accessToken" | "userPassword" | "metaData";
	AuthType string `mapstructure:"auth_type"`
	// Endpoint is the YDB endpoint.
	Endpoint string `mapstructure:"endpoint"`
	// Username is the authentication username.
	Username string `mapstructure:"username"`
	// Password is the authentication password for UserPassword authentication type.
	Password configopaque.String `mapstructure:"password"`
	// AccessToken is the access token for accessToken authentication type.
	AccessToken configopaque.String `mapstructure:"access_token"`
	// ServiceAccountKey is the key for serviceAccountKey authentication type.
	ServiceAccountKey configopaque.String `mapstructure:"service_account_key"`
	// CertificatePath Is the path to PEM file to open grpcs connections.
	CertificatePath string `mapstructure:"certificate_path"`
	// Database is the database name to export.
	Database string `mapstructure:"database"`
	// ConnectionParams is the extra connection parameters with map format. for example compression/dial_timeout
	ConnectionParams map[string]string `mapstructure:"connection_params"`
	// MetricsTable is the config for metrics table.
	MetricsTable TableConfig `mapstructure:"metrics_table"`
	// LogsTable is the config for logs table.
	LogsTable TableConfig `mapstructure:"logs_table"`
	// TracesTable is the config for traces table.
	TracesTable TableConfig `mapstructure:"traces_table"`
}

func DefaultConfig() component.Config {
	queueSettings := exporterhelper.NewDefaultQueueSettings()
	queueSettings.NumConsumers = 1

	return &Config{
		TimeoutSettings:  exporterhelper.NewDefaultTimeoutSettings(),
		QueueSettings:    queueSettings,
		ConnectionParams: map[string]string{},
		Database:         defaultDatabase,
		MetricsTable: TableConfig{
			Name:            defaultMetricsTableName,
			TTL:             defaultTTL,
			PartitionsCount: defaultPartitionsCount,
		},
		LogsTable: TableConfig{
			Name:            defaultLogsTableName,
			TTL:             defaultTTL,
			PartitionsCount: defaultPartitionsCount,
		},
		TracesTable: TableConfig{
			Name:            defaultTracesTableName,
			TTL:             defaultTTL,
			PartitionsCount: defaultPartitionsCount,
		},
	}
}
