// Copyright  OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package k8seventsreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
)

func TestK8sEventToLogData(t *testing.T) {
	k8sEvent := getEvent()

	ld := k8sEventToLogData(zap.NewNop(), k8sEvent)
	rl := ld.ResourceLogs().At(0)
	resourceAttrs := rl.Resource().Attributes()
	lr := rl.InstrumentationLibraryLogs().At(0)
	attrs := lr.LogRecords().At(0).Attributes()
	assert.Equal(t, ld.ResourceLogs().Len(), 1)
	assert.Equal(t, resourceAttrs.Len(), 6)
	assert.Equal(t, attrs.Len(), 7)

	// Count attribute will not be present in the LogData
	k8sEvent.Count = 0
	ld = k8sEventToLogData(zap.NewNop(), k8sEvent)
	assert.Equal(t, ld.ResourceLogs().At(0).InstrumentationLibraryLogs().At(0).LogRecords().At(0).Attributes().Len(), 6)
}

func TestUnknownSeverity(t *testing.T) {
	k8sEvent := getEvent()
	k8sEvent.Type = "Unknown"

	ld := k8sEventToLogData(zap.NewNop(), k8sEvent)
	rl := ld.ResourceLogs().At(0)
	logEntry := rl.InstrumentationLibraryLogs().At(0).LogRecords().At(0)

	assert.Equal(t, logEntry.SeverityNumber(), pdata.SeverityNumberUNDEFINED)
	assert.Equal(t, logEntry.SeverityText(), "")
}
