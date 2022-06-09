// Copyright OpenTelemetry Authors
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

package translation

import (
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	sfxpb "github.com/signalfx/com_signalfx_metrics_protobuf/model"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
)

// This is basically the reverse of the test in the signalfxreceiver for
// converting from events to logs
func TestLogDataToSignalFxEvents(t *testing.T) {
	now := time.Now()
	msec := now.UnixNano() / 1e6

	// Put it on stack or heap so we can take a ref to it.
	userDefinedCat := sfxpb.EventCategory_USER_DEFINED

	buildDefaultSFxEvent := func() *sfxpb.Event {
		return &sfxpb.Event{
			EventType:  "shutdown",
			Timestamp:  msec,
			Category:   &userDefinedCat,
			Dimensions: buildNDimensions(4),
			Properties: mapToEventProps(map[string]interface{}{
				"env":      "prod",
				"isActive": true,
				"rack":     5,
				"temp":     40.5,
			}),
		}
	}

	buildDefaultLogs := func() pdata.Logs {
		logs := pdata.NewLogs()
		resourceLogs := logs.ResourceLogs()
		resourceLog := resourceLogs.AppendEmpty()
		resourceLog.Resource().Attributes().InsertString("k0", "should use ILL attr value instead")
		resourceLog.Resource().Attributes().InsertString("k3", "v3")
		resourceLog.Resource().Attributes().InsertInt("k4", 123)

		ilLogs := resourceLog.InstrumentationLibraryLogs()
		logSlice := ilLogs.AppendEmpty().LogRecords()

		l := logSlice.AppendEmpty()
		l.SetTimestamp(pdata.NewTimestampFromTime(now.Truncate(time.Millisecond)))
		attrs := l.Attributes()

		attrs.InsertString("k0", "v0")
		attrs.InsertString("k1", "v1")
		attrs.InsertString("k2", "v2")

		propMapVal := pdata.NewAttributeValueMap()
		propMap := propMapVal.MapVal()
		propMap.InsertString("env", "prod")
		propMap.InsertBool("isActive", true)
		propMap.InsertInt("rack", 5)
		propMap.InsertDouble("temp", 40.5)
		propMap.Sort()
		attrs.Insert("com.splunk.signalfx.event_properties", propMapVal)
		attrs.Insert("com.splunk.signalfx.event_category", pdata.NewAttributeValueInt(int64(sfxpb.EventCategory_USER_DEFINED)))
		attrs.Insert("com.splunk.signalfx.event_type", pdata.NewAttributeValueString("shutdown"))

		l.Attributes().Sort()

		return logs
	}

	tests := []struct {
		name       string
		sfxEvents  []*sfxpb.Event
		logData    pdata.Logs
		numDropped int
	}{
		{
			name:      "default",
			sfxEvents: []*sfxpb.Event{buildDefaultSFxEvent()},
			logData:   buildDefaultLogs(),
		},
		{
			name: "missing category",
			sfxEvents: func() []*sfxpb.Event {
				e := buildDefaultSFxEvent()
				e.Category = nil
				return []*sfxpb.Event{e}
			}(),
			logData: func() pdata.Logs {
				logs := buildDefaultLogs()
				lrs := logs.ResourceLogs().At(0).InstrumentationLibraryLogs().At(0).LogRecords()
				lrs.At(0).Attributes().Upsert("com.splunk.signalfx.event_category", pdata.NewAttributeValueEmpty())
				return logs
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resource := tt.logData.ResourceLogs().At(0).Resource()
			logSlice := tt.logData.ResourceLogs().At(0).InstrumentationLibraryLogs().At(0).LogRecords()
			events, dropped := LogRecordSliceToSignalFxV2(zap.NewNop(), logSlice, resource.Attributes())
			for i := 0; i < logSlice.Len(); i++ {
				logSlice.At(i).Attributes().Sort()
			}

			for k := range events {
				sort.Slice(events[k].Properties, func(i, j int) bool {
					return events[k].Properties[i].Key < events[k].Properties[j].Key
				})
				sort.Slice(events[k].Dimensions, func(i, j int) bool {
					return events[k].Dimensions[i].Key < events[k].Dimensions[j].Key
				})
			}
			assert.Equal(t, tt.sfxEvents, events)
			assert.Equal(t, tt.numDropped, dropped)
		})
	}
}

func mapToEventProps(m map[string]interface{}) []*sfxpb.Property {
	var out []*sfxpb.Property
	for k, v := range m {
		var pval sfxpb.PropertyValue

		switch t := v.(type) {
		case string:
			pval.StrValue = &t
		case int:
			asInt := int64(t)
			pval.IntValue = &asInt
		case int64:
			pval.IntValue = &t
		case bool:
			pval.BoolValue = &t
		case float64:
			pval.DoubleValue = &t
		default:
			panic(fmt.Sprintf("invalid type: %v", v))
		}

		out = append(out, &sfxpb.Property{
			Key:   k,
			Value: &pval,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Key < out[j].Key
	})
	return out
}

func buildNDimensions(n uint) []*sfxpb.Dimension {
	d := make([]*sfxpb.Dimension, 0, n)
	for i := uint(0); i < n; i++ {
		idx := int(i)
		suffix := strconv.Itoa(idx)
		d = append(d, &sfxpb.Dimension{
			Key:   "k" + suffix,
			Value: "v" + suffix,
		})
	}
	return d
}
