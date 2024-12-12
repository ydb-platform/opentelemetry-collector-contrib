package ydbexporter

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	defaultCfg := WithDefaultConfig(func(c *config.Config) {
		c.Endpoint = defaultEndpoint
	})

	storageID := component.NewIDWithName(component.Type("file_storage"), "ydb")

	tests := []struct {
		id       component.ID
		expected component.Config
	}{
		{
			id:       component.NewIDWithName(metadata.Type, ""),
			expected: defaultCfg,
		},
		{
			id: component.NewIDWithName(metadata.Type, "full"),
			expected: &config.Config{
				AuthType:          "accessToken",
				Endpoint:          defaultEndpoint,
				Username:          "foo",
				Password:          "bar",
				AccessToken:       "token",
				ServiceAccountKey: "key",
				CertificatePath:   "/ydb_certs/ca.pem",
				Database:          "local",
				TimeoutSettings: exporterhelper.TimeoutSettings{
					Timeout: 5 * time.Second,
				},
				ConnectionParams: map[string]string{"param": "param"},
				QueueSettings: exporterhelper.QueueSettings{
					Enabled:      true,
					NumConsumers: 1,
					QueueSize:    100,
					StorageID:    &storageID,
				},
				LogsTable: config.TableConfig{
					TTL:             72 * time.Hour,
					Name:            "otel_logs",
					PartitionsCount: 1,
				},
				TracesTable: config.TableConfig{
					TTL:             72 * time.Hour,
					Name:            "otel_traces",
					PartitionsCount: 1,
				},
				MetricsTable: config.TableConfig{
					TTL:             72 * time.Hour,
					Name:            "otel_metrics",
					PartitionsCount: 1,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, component.UnmarshalConfig(sub, cfg))

			assert.NoError(t, component.ValidateConfig(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}
