// Copyright OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package azuremonitorexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/azuremonitorexporter"

import (
	"time"

	"github.com/microsoft/ApplicationInsights-Go/appinsights/contracts"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
)

const (
	traceIDTag      = "TraceId"
	spanIDTag       = "SpanId"
	categoryNameTag = "CategoryName"
)

var severityLevelMap = map[string]contracts.SeverityLevel{
	"Verbose":     contracts.Verbose,
	"Information": contracts.Information,
	"Warning":     contracts.Warning,
	"Error":       contracts.Error,
	"Critical":    contracts.Critical,
}

type logPacker struct {
	logger *zap.Logger
}

func (packer *logPacker) LogRecordToEnvelope(logRecord pdata.LogRecord) *contracts.Envelope {
	envelope := contracts.NewEnvelope()
	envelope.Tags = make(map[string]string)
	envelope.Time = toTime(logRecord.Timestamp()).Format(time.RFC3339Nano)

	data := contracts.NewData()

	messageData := contracts.NewMessageData()
	messageData.Properties = make(map[string]string)

	messageData.SeverityLevel = packer.toAiSeverityLevel(logRecord.SeverityText())

	messageData.Message = logRecord.Body().StringVal()

	hexTraceID := logRecord.TraceID().HexString()
	messageData.Properties[traceIDTag] = hexTraceID
	envelope.Tags[contracts.OperationId] = hexTraceID

	messageData.Properties[spanIDTag] = logRecord.SpanID().HexString()

	messageData.Properties[categoryNameTag] = logRecord.Name()
	envelope.Name = messageData.EnvelopeName("")

	data.BaseData = messageData
	data.BaseType = messageData.BaseType()
	envelope.Data = data

	packer.sanitize(func() []string { return messageData.Sanitize() })
	packer.sanitize(func() []string { return envelope.Sanitize() })
	packer.sanitize(func() []string { return contracts.SanitizeTags(envelope.Tags) })

	return envelope
}

func (packer *logPacker) sanitize(sanitizeFunc func() []string) {
	for _, warning := range sanitizeFunc() {
		packer.logger.Warn(warning)
	}
}

func (packer *logPacker) toAiSeverityLevel(severityText string) contracts.SeverityLevel {
	if severityLevel, ok := severityLevelMap[severityText]; ok {
		return severityLevel
	}

	packer.logger.Warn("Unknown Severity Level", zap.String("Severity Level", severityText))
	return contracts.Verbose
}

func newLogPacker(logger *zap.Logger) *logPacker {
	packer := &logPacker{
		logger: logger,
	}
	return packer
}
