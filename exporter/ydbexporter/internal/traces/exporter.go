package traces

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/collector/semconv/v1.18.0"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/ydbexporter/internal/db"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
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
		cfg:    &cfg.TracesTable,
	}, nil
}

func (e *Exporter) Start(ctx context.Context, _ component.Host) error {
	err := e.createTable(ctx)
	if err != nil {
		return err
	}
	e.logger.Debug("YDB traces exporter started")
	return nil
}

func (e *Exporter) Shutdown(ctx context.Context) error {
	defer e.logger.Debug("YDB traces exporter stopped")
	if e.client != nil {
		return e.client.Close(ctx)
	}
	return nil
}

func (e *Exporter) createTable(ctx context.Context) error {
	opts := []options.CreateTableOption{
		options.WithColumn("timestamp", types.TypeTimestamp),
		options.WithColumn("traceId", types.TypeUTF8),
		options.WithColumn("spanId", types.Optional(types.TypeUTF8)),
		options.WithColumn("parentSpanId", types.Optional(types.TypeUTF8)),
		options.WithColumn("traceState", types.Optional(types.TypeUTF8)),
		options.WithColumn("spanName", types.Optional(types.TypeUTF8)),
		options.WithColumn("spanKind", types.Optional(types.TypeUTF8)),
		options.WithColumn("serviceName", types.Optional(types.TypeUTF8)),
		options.WithColumn("resourceAttributes", types.Optional(types.TypeJSONDocument)),
		options.WithColumn("scopeName", types.Optional(types.TypeUTF8)),
		options.WithColumn("scopeVersion", types.Optional(types.TypeUTF8)),
		options.WithColumn("spanAttributes", types.Optional(types.TypeJSONDocument)),
		options.WithColumn("duration", types.Optional(types.TypeUint64)),
		options.WithColumn("statusCode", types.Optional(types.TypeUTF8)),
		options.WithColumn("statusMessage", types.Optional(types.TypeUTF8)),
		options.WithColumn("events", types.Optional(types.TypeJSONDocument)),
		options.WithColumn("links", types.Optional(types.TypeJSONDocument)),

		options.WithPrimaryKeyColumn("timestamp", "traceId"),
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

func (e *Exporter) PushData(ctx context.Context, td ptrace.Traces) error {
	var values []types.Value
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpan := td.ResourceSpans().At(i)
		for j := 0; j < resourceSpan.ScopeSpans().Len(); j++ {
			scopeSpans := resourceSpan.ScopeSpans().At(j)
			for k := 0; k < scopeSpans.Spans().Len(); k++ {
				span := scopeSpans.Spans().At(k)
				value, err := e.createRecord(resourceSpan, scopeSpans, span)
				if err != nil {
					return err
				}
				values = append(values, value)
			}
		}
	}
	return e.client.BulkUpsert(ctx, e.cfg.Name, values)
}

func (e *Exporter) createRecord(spans ptrace.ResourceSpans, scopeSpans ptrace.ScopeSpans, span ptrace.Span) (types.Value, error) {
	var serviceName string
	if v, ok := spans.Resource().Attributes().Get(conventions.AttributeServiceName); ok {
		serviceName = v.Str()
	}
	resourceAttributes, err := json.Marshal(spans.Resource().Attributes().AsRaw())
	if err != nil {
		return nil, err
	}
	startTime := span.StartTimestamp().AsTime()
	duration := span.EndTimestamp().AsTime().Sub(startTime)

	spanAttributes := span.Attributes().AsRaw()
	spanAttributesJson, err := json.Marshal(spanAttributes)
	if err != nil {
		return nil, err
	}

	events := convertEvents(span.Events())
	eventsJson, err := json.Marshal(events)
	if err != nil {
		return nil, err
	}

	links := convertLinks(span.Links())
	linksJson, err := json.Marshal(links)
	if err != nil {
		return nil, err
	}

	return types.StructValue(
		types.StructFieldValue("timestamp", types.TimestampValueFromTime(startTime)),
		types.StructFieldValue("traceId", types.UTF8Value(traceutil.TraceIDToHexOrEmptyString(span.TraceID()))),
		types.StructFieldValue("spanId", types.UTF8Value(traceutil.SpanIDToHexOrEmptyString(span.SpanID()))),
		types.StructFieldValue("parentSpanId", types.UTF8Value(traceutil.SpanIDToHexOrEmptyString(span.ParentSpanID()))),
		types.StructFieldValue("traceState", types.UTF8Value(span.TraceState().AsRaw())),
		types.StructFieldValue("spanName", types.UTF8Value(span.Name())),
		types.StructFieldValue("spanKind", types.UTF8Value(traceutil.SpanKindStr(span.Kind()))),
		types.StructFieldValue("serviceName", types.UTF8Value(serviceName)),
		types.StructFieldValue("resourceAttributes", types.JSONDocumentValueFromBytes(resourceAttributes)),
		types.StructFieldValue("scopeName", types.UTF8Value(scopeSpans.Scope().Name())),
		types.StructFieldValue("scopeVersion", types.UTF8Value(scopeSpans.Scope().Version())),
		types.StructFieldValue("spanAttributes", types.JSONDocumentValueFromBytes(spanAttributesJson)),
		types.StructFieldValue("duration", types.Uint64Value(uint64(duration.Nanoseconds()))),
		types.StructFieldValue("statusCode", types.UTF8Value(traceutil.StatusCodeStr(span.Status().Code()))),
		types.StructFieldValue("statusMessage", types.UTF8Value(span.Status().Message())),
		types.StructFieldValue("events", types.JSONDocumentValueFromBytes(eventsJson)),
		types.StructFieldValue("links", types.JSONDocumentValueFromBytes(linksJson)),
	), nil
}

type Event struct {
	Name       string         `json:"name"`
	Time       time.Time      `json:"time"`
	Attributes map[string]any `json:"attributes"`
}

func convertEvents(events ptrace.SpanEventSlice) []Event {
	eventsList := make([]Event, 0, events.Len())
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		eventsList = append(eventsList,
			Event{
				Name:       event.Name(),
				Time:       event.Timestamp().AsTime(),
				Attributes: event.Attributes().AsRaw(),
			})
	}
	return eventsList
}

type Link struct {
	TraceId    string         `json:"traceId"`
	SpanId     string         `json:"spanId"`
	State      string         `json:"state"`
	Attributes map[string]any `json:"attributes"`
}

func convertLinks(links ptrace.SpanLinkSlice) []Link {
	linksList := make([]Link, 0, links.Len())
	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		linksList = append(linksList,
			Link{
				TraceId:    traceutil.TraceIDToHexOrEmptyString(link.TraceID()),
				SpanId:     traceutil.SpanIDToHexOrEmptyString(link.SpanID()),
				State:      link.TraceState().AsRaw(),
				Attributes: link.Attributes().AsRaw(),
			})
	}
	return linksList
}
