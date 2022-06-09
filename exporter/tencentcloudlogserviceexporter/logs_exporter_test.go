// Copyright 2021, OpenTelemetry Authors
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

package tencentcloudlogserviceexporter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/model/pdata"
	conventions "go.opentelemetry.io/collector/model/semconv/v1.5.0"
)

func createSimpleLogData(numberOfLogs int) pdata.Logs {
	logs := pdata.NewLogs()
	logs.ResourceLogs().AppendEmpty() // Add an empty ResourceLogs
	rl := logs.ResourceLogs().AppendEmpty()
	rl.InstrumentationLibraryLogs().AppendEmpty() // Add an empty InstrumentationLibraryLogs
	ill := rl.InstrumentationLibraryLogs().AppendEmpty()

	for i := 0; i < numberOfLogs; i++ {
		ts := pdata.Timestamp(int64(i) * time.Millisecond.Nanoseconds())
		logRecord := ill.LogRecords().AppendEmpty()
		logRecord.Body().SetStringVal("mylog")
		logRecord.Attributes().InsertString(conventions.AttributeServiceName, "myapp")
		logRecord.Attributes().InsertString("my-label", "myapp-type")
		logRecord.Attributes().InsertString(conventions.AttributeHostName, "myhost")
		logRecord.Attributes().InsertString("custom", "custom")
		logRecord.SetTimestamp(ts)
	}
	ill.LogRecords().AppendEmpty()

	return logs
}

func TestNewLogsExporter(t *testing.T) {
	got, err := newLogsExporter(componenttest.NewNopExporterCreateSettings(), &Config{
		ExporterSettings: config.NewExporterSettings(config.NewComponentID(typeStr)),
		Region:           "ap-beijing",
		LogSet:           "demo-logset",
		Topic:            "demo-topic",
	})
	assert.NoError(t, err)
	require.NotNil(t, got)

	// This will put log data to send buffer and return success.
	// Tip: Unit tests require valid access credential.
	_ = got.ConsumeLogs(context.Background(), createSimpleLogData(3))
}

func TestNewFailsWithEmptyLogsExporterName(t *testing.T) {
	got, err := newLogsExporter(componenttest.NewNopExporterCreateSettings(), &Config{})
	assert.NoError(t, err)
	require.NotNil(t, got)
}
