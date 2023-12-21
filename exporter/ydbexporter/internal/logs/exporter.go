package logs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	conventions "go.opentelemetry.io/collector/semconv/v1.18.0"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/db"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
)

var (
	// TODO: move this error to better place
	marshalError = errors.New("cannot marshal data to JSON")
)

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
		cfg:    &cfg.LogsTable,
	}, nil
}

func (e *Exporter) Start(ctx context.Context, _ component.Host) error {
	err := e.createTable(ctx)
	if err != nil {
		return err
	}
	e.logger.Debug("YDB logs exporter started")
	return nil
}

func (e *Exporter) Shutdown(ctx context.Context) error {
	defer e.logger.Debug("YDB logs exporter stopped")
	if e.client != nil {
		return e.client.Close(ctx)
	}
	return nil
}

func (e *Exporter) PushData(ctx context.Context, ld plog.Logs) error {
	rows := make([]types.Value, 0, ld.LogRecordCount())

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		resourceLog := ld.ResourceLogs().At(i)
		for j := 0; j < resourceLog.ScopeLogs().Len(); j++ {
			scopeLog := resourceLog.ScopeLogs().At(j)
			for k := 0; k < scopeLog.LogRecords().Len(); k++ {
				record := scopeLog.LogRecords().At(k)
				row, err := e.createRecord(resourceLog, record, scopeLog)
				if err != nil {
					return err
				}
				rows = append(rows, row)
			}
		}
	}

	return e.client.BulkUpsert(ctx, e.cfg.Name, rows)
}

func (e *Exporter) createRecord(resourceLog plog.ResourceLogs, record plog.LogRecord, scopeLog plog.ScopeLogs) (types.Value, error) {
	var serviceName string
	if v, ok := resourceLog.Resource().Attributes().Get(conventions.AttributeServiceName); ok {
		serviceName = v.Str()
	}
	recordAttributes, err := json.Marshal(record.Attributes().AsRaw())
	if err != nil {
		return nil, fmt.Errorf("%w: %q", marshalError, err)
	}
	scopeAttributes, err := json.Marshal(scopeLog.Scope().Attributes().AsRaw())
	if err != nil {
		return nil, fmt.Errorf("%w: %q", marshalError, err)
	}

	return types.StructValue(
		types.StructFieldValue("timestamp", types.TimestampValueFromTime(record.Timestamp().AsTime())),
		types.StructFieldValue("uuid", types.UTF8Value(uuid.New().String())),
		types.StructFieldValue("traceId", types.UTF8Value(traceutil.TraceIDToHexOrEmptyString(record.TraceID()))),
		types.StructFieldValue("spanId", types.UTF8Value(traceutil.SpanIDToHexOrEmptyString(record.SpanID()))),
		types.StructFieldValue("traceFlags", types.Uint32Value(uint32(record.Flags()))),
		types.StructFieldValue("severityText", types.UTF8Value(record.SeverityText())),
		types.StructFieldValue("severityNumber", types.Int32Value(int32(record.SeverityNumber()))),
		types.StructFieldValue("serviceName", types.UTF8Value(serviceName)),
		types.StructFieldValue("body", types.OptionalValue(types.UTF8Value(record.Body().AsString()))),
		types.StructFieldValue("resourceSchemaUrl", types.UTF8Value(resourceLog.SchemaUrl())),
		types.StructFieldValue("resourceAttributes", types.JSONDocumentValueFromBytes(recordAttributes)),
		types.StructFieldValue("scopeSchemaUrl", types.UTF8Value(scopeLog.SchemaUrl())),
		types.StructFieldValue("scopeName", types.UTF8Value(scopeLog.Scope().Name())),
		types.StructFieldValue("scopeVersion", types.UTF8Value(scopeLog.Scope().Version())),
		types.StructFieldValue("scopeAttributes", types.JSONDocumentValueFromBytes(scopeAttributes)),
	), nil
}

func (e *Exporter) createTable(ctx context.Context) error {
	opts := []options.CreateTableOption{
		options.WithColumn("timestamp", types.TypeTimestamp),
		options.WithColumn("uuid", types.TypeUTF8),
		options.WithColumn("traceId", types.Optional(types.TypeUTF8)),
		options.WithColumn("spanId", types.Optional(types.TypeUTF8)),
		options.WithColumn("traceFlags", types.Optional(types.TypeUint32)),
		options.WithColumn("severityText", types.Optional(types.TypeUTF8)),
		options.WithColumn("severityNumber", types.Optional(types.TypeInt32)),
		options.WithColumn("serviceName", types.Optional(types.TypeUTF8)),
		options.WithColumn("body", types.Optional(types.TypeUTF8)),
		options.WithColumn("resourceSchemaUrl", types.Optional(types.TypeUTF8)),
		options.WithColumn("resourceAttributes", types.Optional(types.TypeJSONDocument)),
		options.WithColumn("scopeSchemaUrl", types.Optional(types.TypeUTF8)),
		options.WithColumn("scopeName", types.Optional(types.TypeUTF8)),
		options.WithColumn("scopeVersion", types.Optional(types.TypeUTF8)),
		options.WithColumn("scopeAttributes", types.Optional(types.TypeJSONDocument)),
		options.WithColumn("logAttributes", types.Optional(types.TypeJSONDocument)),

		options.WithPrimaryKeyColumn("timestamp", "uuid"),
		options.WithAttribute("STORE", "COLUMN"),
	}

	partitionOptions := []options.PartitioningSettingsOption{
		options.WithPartitioningBy([]string{"HASH(timestamp)"}),
	}
	if e.cfg.PartitionsCount > 0 {
		partitionOptions = append(partitionOptions, options.WithMinPartitionsCount(e.cfg.PartitionsCount))
	}
	opts = append(opts, options.WithPartitioningSettings(partitionOptions...))

	if e.cfg.TTL > 0 {
		opts = append(opts,
			options.WithTimeToLiveSettings(
				options.NewTTLSettings().
					ColumnDateType("timestamp").
					ExpireAfter(e.cfg.TTL)))
	}
	return e.client.CreateTable(ctx, e.cfg.Name, opts...)
}
