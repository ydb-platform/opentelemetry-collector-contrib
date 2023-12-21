package metrics

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
)

type gauge struct {
}

func (g *gauge) tableName(config *config.TableConfig) string {
	return config.Name + "_gauge"
}

func (g *gauge) createTableOptions(config *config.TableConfig) []options.CreateTableOption {
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

func (g *gauge) createRecords(resourceMetrics pmetric.ResourceMetrics, scopeMetrics pmetric.ScopeMetrics, metric pmetric.Metric) ([]types.Value, error) {
	recordAttributes, err := json.Marshal(resourceMetrics.Resource().Attributes().AsRaw())
	if err != nil {
		return nil, err
	}
	scopeAttributes, err := json.Marshal(scopeMetrics.Scope().Attributes().AsRaw())
	if err != nil {
		return nil, err
	}
	var records []types.Value
	dataPoints := metric.Gauge().DataPoints()
	for i := 0; i < dataPoints.Len(); i++ {
		dp := metric.Gauge().DataPoints().At(i)

		attributes, err := json.Marshal(dp.Attributes().AsRaw())
		if err != nil {
			return nil, err
		}

		exemplars, err := json.Marshal(convertExemplars(dp.Exemplars()))
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
			types.StructFieldValue("exemplars", types.JSONDocumentValueFromBytes(exemplars)),
		)
		records = append(records, record)
	}
	return records, nil
}

func getValue(dp pmetric.NumberDataPoint) float64 {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		return dp.DoubleValue()
	case pmetric.NumberDataPointValueTypeInt:
		return float64(dp.IntValue())
	case pmetric.NumberDataPointValueTypeEmpty:
		return 0.0
	default:
		return 0.0
	}
}

type Exemplar struct {
	Timestamp          time.Time      `json:"timestamp"`
	Value              float64        `json:"value"`
	SpanId             string         `json:"spanId"`
	TraceId            string         `json:"traceId"`
	FilteredAttributes map[string]any `json:"filteredAttributes,omitempty"`
}

func convertExemplars(exemplars pmetric.ExemplarSlice) []Exemplar {
	var values []Exemplar
	for i := 0; i < exemplars.Len(); i++ {
		e := exemplars.At(i)
		values = append(values,
			Exemplar{
				Timestamp:          e.Timestamp().AsTime(),
				FilteredAttributes: e.FilteredAttributes().AsRaw(),
				Value:              333.0,
				SpanId:             traceutil.SpanIDToHexOrEmptyString(e.SpanID()),
				TraceId:            traceutil.TraceIDToHexOrEmptyString(e.TraceID()),
			})
	}
	return values
}
