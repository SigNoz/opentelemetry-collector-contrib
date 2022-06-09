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

package skywalkingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/skywalkingexporter"

import (
	"strconv"
	"time"

	"go.opentelemetry.io/collector/model/pdata"
	conventions "go.opentelemetry.io/collector/model/semconv/v1.5.0"
	common "skywalking.apache.org/repo/goapi/collect/common/v3"
	logpb "skywalking.apache.org/repo/goapi/collect/logging/v3"
)

const (
	spanIDField            = "spanID"
	severityNumber         = "severityNumber"
	severityText           = "severityText"
	flags                  = "flags"
	instrumentationName    = "otlp.name"
	instrumentationVersion = "otlp.version"
	defaultServiceName     = "otel-collector"
)

func logRecordToLogData(ld pdata.Logs) []*logpb.LogData {
	lds := make([]*logpb.LogData, 0)
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		ills := rl.InstrumentationLibraryLogs()
		resource := rl.Resource()
		for j := 0; j < ills.Len(); j++ {
			ils := ills.At(j)
			logs := ils.LogRecords()
			for k := 0; k < logs.Len(); k++ {
				logData := &logpb.LogData{}
				logData.Tags = &logpb.LogTags{}
				resourceToLogData(resource, logData)
				instrumentationLibraryToLogData(ils.InstrumentationLibrary(), logData)
				mapLogRecordToLogData(logs.At(k), logData)
				lds = append(lds, logData)
			}
		}
	}
	return lds
}

func resourceToLogData(resource pdata.Resource, logData *logpb.LogData) {
	attrs := resource.Attributes()

	if serviceName, ok := attrs.Get(conventions.AttributeServiceName); ok {
		logData.Service = serviceName.AsString()
	} else {
		logData.Service = defaultServiceName
	}

	if serviceInstanceID, ok := attrs.Get(conventions.AttributeServiceInstanceID); ok {
		logData.ServiceInstance = serviceInstanceID.AsString()
	}

	attrs.Range(func(k string, v pdata.AttributeValue) bool {
		logData.Tags.Data = append(logData.Tags.Data, &common.KeyStringValuePair{
			Key:   k,
			Value: v.AsString(),
		})
		return true
	})
}

func instrumentationLibraryToLogData(instrumentationLibrary pdata.InstrumentationLibrary, logData *logpb.LogData) {
	if nameValue := instrumentationLibrary.Name(); nameValue != "" {
		logData.Tags.Data = append(logData.Tags.Data, &common.KeyStringValuePair{
			Key:   instrumentationName,
			Value: nameValue,
		})
	}
	if version := instrumentationLibrary.Version(); version != "" {
		logData.Tags.Data = append(logData.Tags.Data, &common.KeyStringValuePair{
			Key:   instrumentationVersion,
			Value: version,
		})
	}
}

func mapLogRecordToLogData(lr pdata.LogRecord, logData *logpb.LogData) {
	if lr.Body().Type() == pdata.AttributeValueTypeEmpty {
		return
	}

	if timestamp := lr.Timestamp(); timestamp > 0 {
		logData.Timestamp = lr.Timestamp().AsTime().UnixNano() / int64(time.Millisecond)
	}

	if sn := strconv.FormatInt(int64(lr.SeverityNumber()), 10); sn != "" {
		logData.Tags.Data = append(logData.Tags.Data, &common.KeyStringValuePair{
			Key:   severityNumber,
			Value: sn,
		})
	}

	if st := lr.SeverityText(); st != "" {
		logData.Tags.Data = append(logData.Tags.Data, &common.KeyStringValuePair{
			Key:   severityText,
			Value: st,
		})
	}

	lr.Attributes().Range(func(k string, v pdata.AttributeValue) bool {
		logData.Tags.Data = append(logData.Tags.Data, &common.KeyStringValuePair{
			Key:   k,
			Value: v.AsString(),
		})
		return true
	})

	logData.Body = &logpb.LogDataBody{
		Type: "body-type",
		Content: &logpb.LogDataBody_Text{
			Text: &logpb.TextLog{
				Text: lr.Body().AsString(),
			}},
	}

	if flag := strconv.FormatUint(uint64(lr.Flags()), 16); flag != "" {
		logData.Tags.Data = append(logData.Tags.Data, &common.KeyStringValuePair{
			Key:   flags,
			Value: flag,
		})
	}

	if traceID := lr.TraceID().HexString(); traceID != "" {
		logData.TraceContext = &logpb.TraceContext{TraceId: traceID}
	}

	if spanID := lr.SpanID().HexString(); spanID != "" {
		logData.Tags.Data = append(logData.Tags.Data, &common.KeyStringValuePair{
			Key:   spanIDField,
			Value: spanID,
		})
	}
}
