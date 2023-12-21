package metrics

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"
)

type sum struct {
}

func (g *sum) tableName(config *config.TableConfig) string {
	return config.Name + "_sum"
}

func (g *sum) createTableOptions(config *config.TableConfig) []options.CreateTableOption {
	opts := []options.CreateTableOption{
		options.WithColumn("timestamp", types.TypeTimestamp),
		options.WithColumn("metricName", types.TypeUTF8),
		options.WithColumn("uuid", types.TypeUTF8),
		options.WithColumn("startTimestamp", types.Optional(types.TypeTimestamp)),

		options.WithColumn("resourceAttributes", types.Optional(types.TypeJSONDocument)),
		options.WithColumn("resourceSchemaUrl", types.Optional(types.TypeUTF8)),
		options.WithColumn("scopeName", types.Optional(types.TypeUTF8)),
		options.WithColumn("scopeVersion", types.Optional(types.TypeUTF8)),
		options.WithColumn("scopeAttributes", types.Optional(types.TypeJSONDocument)),
		options.WithColumn("scopeSchemaUrl", types.Optional(types.TypeUTF8)),

		options.WithColumn("metricDescription", types.Optional(types.TypeUTF8)),
		options.WithColumn("metricUnit", types.Optional(types.TypeUTF8)),
		options.WithColumn("attributes", types.Optional(types.TypeJSONDocument)),
		options.WithColumn("value", types.Optional(types.TypeDouble)),
		options.WithColumn("flags", types.Optional(types.TypeUint32)),
		options.WithColumn("aggTemp", types.Optional(types.TypeInt32)),
		options.WithColumn("isMonotonic", types.Optional(types.TypeBool)),
		options.WithColumn("exemplars", types.Optional(types.TypeJSONDocument)),

		options.WithPrimaryKeyColumn("timestamp", "uuid", "metricName"),
		options.WithAttribute("STORE", "COLUMN"),
	}

	partitionOptions := []options.PartitioningSettingsOption{
		options.WithPartitioningBy([]string{"HASH(timestamp)"}),
	}
	if config.PartitionsCount > 0 {
		partitionOptions = append(partitionOptions, options.WithMinPartitionsCount(config.PartitionsCount))
	}
	opts = append(opts, options.WithPartitioningSettings(partitionOptions...))

	if config.TTL > 0 {
		opts = append(opts,
			options.WithTimeToLiveSettings(
				options.NewTTLSettings().
					ColumnDateType("timestamp").
					ExpireAfter(config.TTL)))
	}
	return opts
}

func (g *sum) createRecords(resourceMetrics pmetric.ResourceMetrics, scopeMetrics pmetric.ScopeMetrics, metric pmetric.Metric) ([]types.Value, error) {
	recordAttributes, err := json.Marshal(resourceMetrics.Resource().Attributes().AsRaw())
	if err != nil {
		return nil, err
	}
	scopeAttributes, err := json.Marshal(scopeMetrics.Scope().Attributes().AsRaw())
	if err != nil {
		return nil, err
	}
	var records []types.Value
	dataPoints := metric.Sum().DataPoints()
	for i := 0; i < dataPoints.Len(); i++ {
		dp := metric.Sum().DataPoints().At(i)
		attributes, err := json.Marshal(dp.Attributes().AsRaw())
		if err != nil {
			return nil, err
		}

		record := types.StructValue(
			types.StructFieldValue("startTimestamp", types.TimestampValueFromTime(dp.StartTimestamp().AsTime())),
			types.StructFieldValue("timestamp", types.TimestampValueFromTime(dp.Timestamp().AsTime())),
			types.StructFieldValue("uuid", types.UTF8Value(uuid.New().String())),
			types.StructFieldValue("resourceSchemaUrl", types.UTF8Value(resourceMetrics.SchemaUrl())),
			types.StructFieldValue("resourceAttributes", types.JSONDocumentValueFromBytes(recordAttributes)),
			types.StructFieldValue("scopeName", types.UTF8Value(scopeMetrics.Scope().Name())),
			types.StructFieldValue("scopeVersion", types.UTF8Value(scopeMetrics.Scope().Version())),
			types.StructFieldValue("scopeAttributes", types.JSONDocumentValueFromBytes(scopeAttributes)),
			types.StructFieldValue("scopeSchemaUrl", types.UTF8Value(scopeMetrics.SchemaUrl())),
			types.StructFieldValue("metricName", types.UTF8Value(metric.Name())),
			types.StructFieldValue("metricDescription", types.UTF8Value(metric.Description())),
			types.StructFieldValue("metricUnit", types.UTF8Value(metric.Unit())),
			types.StructFieldValue("attributes", types.JSONDocumentValueFromBytes(attributes)),
			types.StructFieldValue("value", types.DoubleValue(getValue(dp))),
			types.StructFieldValue("flags", types.Uint32Value(uint32(dp.Flags()))),
			types.StructFieldValue("aggTemp", types.Int32Value(int32(metric.Sum().AggregationTemporality()))),
			types.StructFieldValue("isMonotonic", types.BoolValue(metric.Sum().IsMonotonic())),
		)
		records = append(records, record)
	}
	return records, nil
}
