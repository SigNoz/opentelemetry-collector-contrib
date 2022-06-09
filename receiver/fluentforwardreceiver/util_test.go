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

package fluentforwardreceiver

import (
	"go.opentelemetry.io/collector/model/pdata"
)

// Log is a convenience struct for constructing logs for tests.
// See Logs for rationale.
type Log struct {
	Timestamp  int64
	Body       pdata.AttributeValue
	Attributes map[string]pdata.AttributeValue
}

// Logs is a convenience function for constructing logs for tests in a way that is
// relatively easy to read and write declaratively compared to the highly
// imperative and verbose method of using pdata directly.
// Attributes are sorted by key name.
func Logs(recs ...Log) pdata.Logs {
	out := pdata.NewLogs()
	logSlice := out.ResourceLogs().AppendEmpty().InstrumentationLibraryLogs().AppendEmpty().LogRecords()
	logSlice.EnsureCapacity(len(recs))
	for i := range recs {
		l := logSlice.AppendEmpty()
		recs[i].Body.CopyTo(l.Body())
		l.SetTimestamp(pdata.Timestamp(recs[i].Timestamp))
		pdata.NewAttributeMapFromMap(recs[i].Attributes).CopyTo(l.Attributes())
		l.Attributes().Sort()
	}

	return out
}
