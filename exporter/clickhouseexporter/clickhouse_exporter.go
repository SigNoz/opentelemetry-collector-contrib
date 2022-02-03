// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clickhouseexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/model/pdata"
	conventions "go.opentelemetry.io/collector/model/semconv/v1.5.0"
	"go.uber.org/zap"
)

// Crete new exporter.
func newExporter(cfg config.Exporter, logger *zap.Logger) (*storage, error) {

	configClickHouse := cfg.(*Config)

	f := ClickHouseNewFactory(configClickHouse.Migrations, configClickHouse.Datasource)

	err := f.Initialize(logger)
	if err != nil {
		return nil, err
	}

	spanWriter, err := f.CreateSpanWriter()
	if err != nil {
		return nil, err
	}
	storage := storage{Writer: spanWriter}

	return &storage, nil
}

type storage struct {
	Writer Writer
}

func makeJaegerProtoReferences(
	links pdata.SpanLinkSlice,
	parentSpanID pdata.SpanID,
	traceID pdata.TraceID,
) ([]OtelSpanRef, error) {

	parentSpanIDSet := len(parentSpanID.Bytes()) != 0
	if !parentSpanIDSet && links.Len() == 0 {
		return nil, nil
	}

	refsCount := links.Len()
	if parentSpanIDSet {
		refsCount++
	}

	refs := make([]OtelSpanRef, 0, refsCount)

	// Put parent span ID at the first place because usually backends look for it
	// as the first CHILD_OF item in the model.SpanRef slice.
	if parentSpanIDSet {

		refs = append(refs, OtelSpanRef{
			TraceId: traceID.HexString(),
			SpanId:  parentSpanID.HexString(),
			RefType: "CHILD_OF",
		})
	}

	for i := 0; i < links.Len(); i++ {
		link := links.At(i)

		refs = append(refs, OtelSpanRef{
			TraceId: link.TraceID().HexString(),
			SpanId:  link.SpanID().HexString(),

			// Since Jaeger RefType is not captured in internal data,
			// use SpanRefType_FOLLOWS_FROM by default.
			// SpanRefType_CHILD_OF supposed to be set only from parentSpanID.
			RefType: "FOLLOWS_FROM",
		})
	}

	return refs, nil
}

// ServiceNameForResource gets the service name for a specified Resource.
// TODO: Find a better package for this function.
func ServiceNameForResource(resource pdata.Resource) string {
	// if resource.IsNil() {
	// 	return "<nil-resource>"
	// }

	service, found := resource.Attributes().Get(conventions.AttributeServiceName)
	if !found {
		return "<nil-service-name>"
	}

	return service.StringVal()
}

func populateOtherDimensions(attributes pdata.AttributeMap, span *Span) {

	attributes.Range(func(k string, v pdata.AttributeValue) bool {
		if k == "http.status_code" {
			span.StatusCode = v.IntVal()
		}
		if k == "http.url" && span.Kind == 2 {
			span.ExternalHttpUrl = v.StringVal()
		}
		if k == "http.method" && span.Kind == 2 {
			span.ExternalHttpMethod = v.StringVal()
		}
		if k == "http.status_code" {
			span.HttpCode = strconv.FormatInt(v.IntVal(), 10)
		}
		if k == "http.url" {
			span.HttpUrl = v.StringVal()
		}
		if k == "http.method" {
			span.HttpMethod = v.StringVal()
		}
		if k == "http.route" {
			span.HttpMethod = v.StringVal()
		}
		if k == "http.host" {
			span.HttpHost = v.StringVal()
		}
		if k == "messaging.system" {
			span.MsgSystem = v.StringVal()
		}
		if k == "messaging.operation" {
			span.MsgOperation = v.StringVal()
		}
		if k == "component" {
			span.Component = v.StringVal()
		}
		if k == "db.system" {
			span.DBSystem = v.StringVal()
		}
		if k == "db.name" {
			span.DBName = v.StringVal()
		}
		if k == "db.operation" {
			span.DBOperation = v.StringVal()
		}
		if k == "peer.service" {
			span.PeerService = v.StringVal()
		}

		return true

	})

}

func populateEvents(events pdata.SpanEventSlice, span *Span) {
	for i := 0; i < events.Len(); i++ {
		event := Event{}
		event.Name = events.At(i).Name()
		event.TimeUnixNano = uint64(events.At(i).Timestamp())
		event.AttributeMap = map[string]string{}
		events.At(i).Attributes().Range(func(k string, v pdata.AttributeValue) bool {
			if v.Type().String() == "INT" {
				event.AttributeMap[k] = strconv.FormatInt(v.IntVal(), 10)
			} else {
				event.AttributeMap[k] = v.StringVal()
			}
			return true
		})
		stringEvent, _ := json.Marshal(event)
		span.Events = append(span.Events, string(stringEvent))
		if event.Name == "exception" {
			span.ErrorEvent = event
			uuidWithHyphen := uuid.New()
			uuid := strings.Replace(uuidWithHyphen.String(), "-", "", -1)
			span.ErrorID = uuid
		}
	}
}

func newStructuredSpan(otelSpan pdata.Span, ServiceName string) *Span {

	durationNano := uint64(otelSpan.EndTimestamp() - otelSpan.StartTimestamp())

	attributes := otelSpan.Attributes()

	var tags []string
	var tagsKeys []string
	var tagsValues []string
	var tag string

	attributes.Range(func(k string, v pdata.AttributeValue) bool {
		v.StringVal()
		if v.Type().String() == "INT" {
			tag = fmt.Sprintf("%s:%d", k, v.IntVal())
			tagsValues = append(tagsValues, strconv.FormatInt(v.IntVal(), 10))
		} else {
			tag = fmt.Sprintf("%s:%s", k, v.StringVal())
			tagsValues = append(tagsValues, v.StringVal())
		}

		tags = append(tags, tag)
		tagsKeys = append(tagsKeys, k)
		return true

	})

	references, _ := makeJaegerProtoReferences(otelSpan.Links(), otelSpan.ParentSpanID(), otelSpan.TraceID())

	var span *Span = &Span{
		TraceId:           otelSpan.TraceID().HexString(),
		SpanId:            otelSpan.SpanID().HexString(),
		ParentSpanId:      otelSpan.ParentSpanID().HexString(),
		Name:              otelSpan.Name(),
		StartTimeUnixNano: uint64(otelSpan.StartTimestamp()),
		DurationNano:      durationNano,
		ServiceName:       ServiceName,
		Kind:              int32(otelSpan.Kind()),
		References:        references,
		Tags:              tags,
		TagsKeys:          tagsKeys,
		TagsValues:        tagsValues,
	}
	span.StatusCode = int64(otelSpan.Status().Code())

	populateOtherDimensions(attributes, span)
	populateEvents(otelSpan.Events(), span)

	return span
}

// traceDataPusher implements OTEL exporterhelper.traceDataPusher
func (s *storage) pushTraceData(ctx context.Context, td pdata.Traces) error {

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		// fmt.Printf("ResourceSpans #%d\n", i)
		rs := rss.At(i)

		serviceName := ServiceNameForResource(rs.Resource())

		ilss := rs.InstrumentationLibrarySpans()
		for j := 0; j < ilss.Len(); j++ {
			// fmt.Printf("InstrumentationLibrarySpans #%d\n", j)
			ils := ilss.At(j)

			spans := ils.Spans()

			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				// traceID := hex.EncodeToString(span.TraceID())
				structuredSpan := newStructuredSpan(span, serviceName)
				err := s.Writer.WriteSpan(structuredSpan)
				if err != nil {
					zap.S().Error("Error in writing spans to clickhouse: ", err)
				}
			}
		}
	}

	return nil
}
