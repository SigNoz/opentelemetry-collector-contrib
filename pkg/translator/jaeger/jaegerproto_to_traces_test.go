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

package jaeger

import (
	"encoding/binary"
	"strconv"
	"testing"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/model/pdata"
	conventions "go.opentelemetry.io/collector/model/semconv/v1.5.0"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/idutils"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/testdata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/tracetranslator"
)

// Use timespamp with microsecond granularity to work well with jaeger thrift translation
var (
	testSpanStartTime      = time.Date(2020, 2, 11, 20, 26, 12, 321000, time.UTC)
	testSpanStartTimestamp = pdata.NewTimestampFromTime(testSpanStartTime)
	testSpanEventTime      = time.Date(2020, 2, 11, 20, 26, 13, 123000, time.UTC)
	testSpanEventTimestamp = pdata.NewTimestampFromTime(testSpanEventTime)
	testSpanEndTime        = time.Date(2020, 2, 11, 20, 26, 13, 789000, time.UTC)
	testSpanEndTimestamp   = pdata.NewTimestampFromTime(testSpanEndTime)
)

func TestCodeFromAttr(t *testing.T) {
	tests := []struct {
		name string
		attr pdata.AttributeValue
		code int64
		err  error
	}{
		{
			name: "ok-string",
			attr: pdata.NewAttributeValueString("0"),
			code: 0,
		},

		{
			name: "ok-int",
			attr: pdata.NewAttributeValueInt(1),
			code: 1,
		},

		{
			name: "wrong-type",
			attr: pdata.NewAttributeValueBool(true),
			code: 0,
			err:  errType,
		},

		{
			name: "invalid-string",
			attr: pdata.NewAttributeValueString("inf"),
			code: 0,
			err:  strconv.ErrSyntax,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			code, err := codeFromAttr(test.attr)
			if test.err != nil {
				assert.ErrorIs(t, err, test.err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, test.code, code)
		})
	}
}

func TestGetStatusCodeFromHTTPStatusAttr(t *testing.T) {
	tests := []struct {
		name string
		attr pdata.AttributeValue
		code pdata.StatusCode
	}{
		{
			name: "string-unknown",
			attr: pdata.NewAttributeValueString("10"),
			code: pdata.StatusCodeError,
		},

		{
			name: "string-ok",
			attr: pdata.NewAttributeValueString("101"),
			code: pdata.StatusCodeUnset,
		},

		{
			name: "int-not-found",
			attr: pdata.NewAttributeValueInt(404),
			code: pdata.StatusCodeError,
		},
		{
			name: "int-invalid-arg",
			attr: pdata.NewAttributeValueInt(408),
			code: pdata.StatusCodeError,
		},

		{
			name: "int-internal",
			attr: pdata.NewAttributeValueInt(500),
			code: pdata.StatusCodeError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			code, err := getStatusCodeFromHTTPStatusAttr(test.attr)
			assert.NoError(t, err)
			assert.Equal(t, test.code, code)
		})
	}
}

func TestJTagsToInternalAttributes(t *testing.T) {
	tags := []model.KeyValue{
		{
			Key:   "bool-val",
			VType: model.ValueType_BOOL,
			VBool: true,
		},
		{
			Key:    "int-val",
			VType:  model.ValueType_INT64,
			VInt64: 123,
		},
		{
			Key:   "string-val",
			VType: model.ValueType_STRING,
			VStr:  "abc",
		},
		{
			Key:      "double-val",
			VType:    model.ValueType_FLOAT64,
			VFloat64: 1.23,
		},
		{
			Key:     "binary-val",
			VType:   model.ValueType_BINARY,
			VBinary: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x64, 0x7D, 0x98},
		},
	}

	expected := pdata.NewAttributeMap()
	expected.InsertBool("bool-val", true)
	expected.InsertInt("int-val", 123)
	expected.InsertString("string-val", "abc")
	expected.InsertDouble("double-val", 1.23)
	expected.InsertString("binary-val", "AAAAAABkfZg=")

	got := pdata.NewAttributeMap()
	jTagsToInternalAttributes(tags, got)

	require.EqualValues(t, expected, got)
}

func TestProtoToTraces(t *testing.T) {

	tests := []struct {
		name string
		jb   []*model.Batch
		td   pdata.Traces
	}{
		{
			name: "empty",
			jb:   []*model.Batch{},
			td:   pdata.NewTraces(),
		},

		{
			name: "no-spans",
			jb: []*model.Batch{
				{
					Process: generateProtoProcess(),
				}},
			td: generateTracesResourceOnly(),
		},

		{
			name: "no-resource-attrs",
			jb: []*model.Batch{
				{
					Process: &model.Process{
						ServiceName: tracetranslator.ResourceNoServiceName,
					},
				}},
			td: generateTracesResourceOnlyWithNoAttrs(),
		},

		{
			name: "one-span-no-resources",
			jb: []*model.Batch{
				{
					Process: &model.Process{
						ServiceName: tracetranslator.ResourceNoServiceName,
					},
					Spans: []*model.Span{
						generateProtoSpanWithTraceState(),
					},
				}},
			td: generateTracesOneSpanNoResourceWithTraceState(),
		},
		{
			name: "two-spans-child-parent",
			jb: []*model.Batch{
				{
					Process: &model.Process{
						ServiceName: tracetranslator.ResourceNoServiceName,
					},
					Spans: []*model.Span{
						generateProtoSpan(),
						generateProtoChildSpan(),
					},
				}},
			td: generateTracesTwoSpansChildParent(),
		},

		{
			name: "two-spans-with-follower",
			jb: []*model.Batch{
				{
					Process: &model.Process{
						ServiceName: tracetranslator.ResourceNoServiceName,
					},
					Spans: []*model.Span{
						generateProtoSpan(),
						generateProtoFollowerSpan(),
					},
				}},
			td: generateTracesTwoSpansWithFollower(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			td, err := ProtoToTraces(test.jb)
			assert.NoError(t, err)
			assert.EqualValues(t, test.td, td)
		})
	}
}

func TestProtoBatchToInternalTracesWithTwoLibraries(t *testing.T) {
	jb := &model.Batch{
		Process: &model.Process{
			ServiceName: tracetranslator.ResourceNoServiceName,
		},
		Spans: []*model.Span{
			{
				StartTime:     testSpanStartTime,
				Duration:      testSpanEndTime.Sub(testSpanStartTime),
				OperationName: "operation2",
				Tags: []model.KeyValue{
					{
						Key:   conventions.InstrumentationLibraryName,
						VType: model.ValueType_STRING,
						VStr:  "library2",
					}, {
						Key:   conventions.InstrumentationLibraryVersion,
						VType: model.ValueType_STRING,
						VStr:  "0.42.0",
					},
				},
			},
			{
				TraceID:       model.NewTraceID(0, 0),
				StartTime:     testSpanStartTime,
				Duration:      testSpanEndTime.Sub(testSpanStartTime),
				OperationName: "operation1",
				Tags: []model.KeyValue{
					{
						Key:   conventions.InstrumentationLibraryName,
						VType: model.ValueType_STRING,
						VStr:  "library1",
					}, {
						Key:   conventions.InstrumentationLibraryVersion,
						VType: model.ValueType_STRING,
						VStr:  "0.42.0",
					},
				},
			},
		},
	}
	expected := generateTracesTwoSpansFromTwoLibraries()
	library1Span := expected.ResourceSpans().At(0).InstrumentationLibrarySpans().At(0)
	library2Span := expected.ResourceSpans().At(0).InstrumentationLibrarySpans().At(1)

	actual, err := ProtoToTraces([]*model.Batch{jb})
	assert.NoError(t, err)

	assert.Equal(t, actual.ResourceSpans().Len(), 1)
	assert.Equal(t, actual.ResourceSpans().At(0).InstrumentationLibrarySpans().Len(), 2)

	ils0 := actual.ResourceSpans().At(0).InstrumentationLibrarySpans().At(0)
	ils1 := actual.ResourceSpans().At(0).InstrumentationLibrarySpans().At(1)
	if ils0.InstrumentationLibrary().Name() == "library1" {
		assert.EqualValues(t, library1Span, ils0)
		assert.EqualValues(t, library2Span, ils1)
	} else {
		assert.EqualValues(t, library1Span, ils1)
		assert.EqualValues(t, library2Span, ils0)
	}
}

func TestSetInternalSpanStatus(t *testing.T) {

	emptyStatus := pdata.NewSpanStatus()

	okStatus := pdata.NewSpanStatus()
	okStatus.SetCode(pdata.StatusCodeOk)

	errorStatus := pdata.NewSpanStatus()
	errorStatus.SetCode(pdata.StatusCodeError)

	errorStatusWithMessage := pdata.NewSpanStatus()
	errorStatusWithMessage.SetCode(pdata.StatusCodeError)
	errorStatusWithMessage.SetMessage("Error: Invalid argument")

	errorStatusWith404Message := pdata.NewSpanStatus()
	errorStatusWith404Message.SetCode(pdata.StatusCodeError)
	errorStatusWith404Message.SetMessage("HTTP 404: Not Found")

	tests := []struct {
		name             string
		attrs            pdata.AttributeMap
		status           pdata.SpanStatus
		attrsModifiedLen int // Length of attributes map after dropping converted fields
	}{
		{
			name:             "No tags set -> OK status",
			attrs:            pdata.NewAttributeMapFromMap(map[string]pdata.AttributeValue{}),
			status:           emptyStatus,
			attrsModifiedLen: 0,
		},
		{
			name: "error tag set -> Error status",
			attrs: pdata.NewAttributeMapFromMap(map[string]pdata.AttributeValue{
				tracetranslator.TagError: pdata.NewAttributeValueBool(true),
			}),
			status:           errorStatus,
			attrsModifiedLen: 0,
		},
		{
			name: "status.code is set as string",
			attrs: pdata.NewAttributeMapFromMap(map[string]pdata.AttributeValue{
				conventions.OtelStatusCode: pdata.NewAttributeValueString(statusOk),
			}),
			status:           okStatus,
			attrsModifiedLen: 0,
		},
		{
			name: "status.code, status.message and error tags are set",
			attrs: pdata.NewAttributeMapFromMap(map[string]pdata.AttributeValue{
				tracetranslator.TagError:          pdata.NewAttributeValueBool(true),
				conventions.OtelStatusCode:        pdata.NewAttributeValueString(statusError),
				conventions.OtelStatusDescription: pdata.NewAttributeValueString("Error: Invalid argument"),
			}),
			status:           errorStatusWithMessage,
			attrsModifiedLen: 0,
		},
		{
			name: "http.status_code tag is set as string",
			attrs: pdata.NewAttributeMapFromMap(map[string]pdata.AttributeValue{
				conventions.AttributeHTTPStatusCode: pdata.NewAttributeValueString("404"),
			}),
			status:           errorStatus,
			attrsModifiedLen: 1,
		},
		{
			name: "http.status_code, http.status_message and error tags are set",
			attrs: pdata.NewAttributeMapFromMap(map[string]pdata.AttributeValue{
				tracetranslator.TagError:            pdata.NewAttributeValueBool(true),
				conventions.AttributeHTTPStatusCode: pdata.NewAttributeValueInt(404),
				tracetranslator.TagHTTPStatusMsg:    pdata.NewAttributeValueString("HTTP 404: Not Found"),
			}),
			status:           errorStatusWith404Message,
			attrsModifiedLen: 2,
		},
		{
			name: "status.code has precedence over http.status_code.",
			attrs: pdata.NewAttributeMapFromMap(map[string]pdata.AttributeValue{
				conventions.OtelStatusCode:          pdata.NewAttributeValueString(statusOk),
				conventions.AttributeHTTPStatusCode: pdata.NewAttributeValueInt(500),
				tracetranslator.TagHTTPStatusMsg:    pdata.NewAttributeValueString("Server Error"),
			}),
			status:           okStatus,
			attrsModifiedLen: 2,
		},
		{
			name: "Ignore http.status_code == 200 if error set to true.",
			attrs: pdata.NewAttributeMapFromMap(map[string]pdata.AttributeValue{
				tracetranslator.TagError:            pdata.NewAttributeValueBool(true),
				conventions.AttributeHTTPStatusCode: pdata.NewAttributeValueInt(200),
			}),
			status:           errorStatus,
			attrsModifiedLen: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status := pdata.NewSpanStatus()
			setInternalSpanStatus(test.attrs, status)
			assert.EqualValues(t, test.status, status)
			assert.Equal(t, test.attrsModifiedLen, test.attrs.Len())
		})
	}
}

func TestProtoBatchesToInternalTraces(t *testing.T) {
	batches := []*model.Batch{
		{
			Process: generateProtoProcess(),
			Spans: []*model.Span{
				generateProtoSpan(),
			},
		},
		{
			Spans: []*model.Span{
				generateProtoSpan(),
				generateProtoChildSpan(),
			},
		},
		{
			// should be skipped
			Spans: []*model.Span{},
		},
	}

	expected := generateTracesOneSpanNoResource()
	resource := generateTracesResourceOnly().ResourceSpans().At(0).Resource()
	resource.CopyTo(expected.ResourceSpans().At(0).Resource())
	tgt := expected.ResourceSpans().AppendEmpty()
	twoSpans := generateTracesTwoSpansChildParent().ResourceSpans().At(0)
	twoSpans.CopyTo(tgt)

	got, err := ProtoToTraces(batches)
	assert.NoError(t, err)
	assert.EqualValues(t, expected, got)
}

func TestJSpanKindToInternal(t *testing.T) {
	tests := []struct {
		jSpanKind    string
		otlpSpanKind pdata.SpanKind
	}{
		{
			jSpanKind:    "client",
			otlpSpanKind: pdata.SpanKindClient,
		},
		{
			jSpanKind:    "server",
			otlpSpanKind: pdata.SpanKindServer,
		},
		{
			jSpanKind:    "producer",
			otlpSpanKind: pdata.SpanKindProducer,
		},
		{
			jSpanKind:    "consumer",
			otlpSpanKind: pdata.SpanKindConsumer,
		},
		{
			jSpanKind:    "internal",
			otlpSpanKind: pdata.SpanKindInternal,
		},
		{
			jSpanKind:    "all-others",
			otlpSpanKind: pdata.SpanKindUnspecified,
		},
	}

	for _, test := range tests {
		t.Run(test.jSpanKind, func(t *testing.T) {
			assert.Equal(t, test.otlpSpanKind, jSpanKindToInternal(test.jSpanKind))
		})
	}
}

func generateTracesResourceOnly() pdata.Traces {
	td := testdata.GenerateTracesOneEmptyResourceSpans()
	rs := td.ResourceSpans().At(0).Resource()
	rs.Attributes().InsertString(conventions.AttributeServiceName, "service-1")
	rs.Attributes().InsertInt("int-attr-1", 123)
	return td
}

func generateTracesResourceOnlyWithNoAttrs() pdata.Traces {
	return testdata.GenerateTracesOneEmptyResourceSpans()
}

func generateProtoProcess() *model.Process {
	return &model.Process{
		ServiceName: "service-1",
		Tags: []model.KeyValue{
			{
				Key:    "int-attr-1",
				VType:  model.ValueType_INT64,
				VInt64: 123,
			},
		},
	}
}

func generateTracesOneSpanNoResource() pdata.Traces {
	td := testdata.GenerateTracesOneSpanNoResource()
	span := td.ResourceSpans().At(0).InstrumentationLibrarySpans().At(0).Spans().At(0)
	span.SetSpanID(pdata.NewSpanID([8]byte{0xAF, 0xAE, 0xAD, 0xAC, 0xAB, 0xAA, 0xA9, 0xA8}))
	span.SetTraceID(pdata.NewTraceID(
		[16]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF, 0x80}))
	span.SetDroppedAttributesCount(0)
	span.SetDroppedEventsCount(0)
	span.SetStartTimestamp(testSpanStartTimestamp)
	span.SetEndTimestamp(testSpanEndTimestamp)
	span.SetKind(pdata.SpanKindClient)
	span.Events().At(0).SetTimestamp(testSpanEventTimestamp)
	span.Events().At(0).SetDroppedAttributesCount(0)
	span.Events().At(0).SetName("event-with-attr")
	span.Events().At(1).SetTimestamp(testSpanEventTimestamp)
	span.Events().At(1).SetDroppedAttributesCount(0)
	span.Events().At(1).SetName("")
	span.Events().At(1).Attributes().InsertInt("attr-int", 123)
	return td
}

func generateTracesWithLibraryInfo() pdata.Traces {
	td := generateTracesOneSpanNoResource()
	rs0 := td.ResourceSpans().At(0)
	rs0ils0 := rs0.InstrumentationLibrarySpans().At(0)
	rs0ils0.InstrumentationLibrary().SetName("io.opentelemetry.test")
	rs0ils0.InstrumentationLibrary().SetVersion("0.42.0")
	return td
}

func generateTracesOneSpanNoResourceWithTraceState() pdata.Traces {
	td := generateTracesOneSpanNoResource()
	span := td.ResourceSpans().At(0).InstrumentationLibrarySpans().At(0).Spans().At(0)
	span.SetTraceState("lasterror=f39cd56cc44274fd5abd07ef1164246d10ce2955")
	return td
}

func generateProtoSpan() *model.Span {
	return &model.Span{
		TraceID: model.NewTraceID(
			binary.BigEndian.Uint64([]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}),
			binary.BigEndian.Uint64([]byte{0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF, 0x80}),
		),
		SpanID:        model.NewSpanID(binary.BigEndian.Uint64([]byte{0xAF, 0xAE, 0xAD, 0xAC, 0xAB, 0xAA, 0xA9, 0xA8})),
		OperationName: "operationA",
		StartTime:     testSpanStartTime,
		Duration:      testSpanEndTime.Sub(testSpanStartTime),
		Logs: []model.Log{
			{
				Timestamp: testSpanEventTime,
				Fields: []model.KeyValue{
					{
						Key:   tracetranslator.TagMessage,
						VType: model.ValueType_STRING,
						VStr:  "event-with-attr",
					},
					{
						Key:   "span-event-attr",
						VType: model.ValueType_STRING,
						VStr:  "span-event-attr-val",
					},
				},
			},
			{
				Timestamp: testSpanEventTime,
				Fields: []model.KeyValue{
					{
						Key:    "attr-int",
						VType:  model.ValueType_INT64,
						VInt64: 123,
					},
				},
			},
		},
		Tags: []model.KeyValue{
			{
				Key:   tracetranslator.TagSpanKind,
				VType: model.ValueType_STRING,
				VStr:  string(tracetranslator.OpenTracingSpanKindClient),
			},
			{
				Key:   conventions.OtelStatusCode,
				VType: model.ValueType_STRING,
				VStr:  statusError,
			},
			{
				Key:   tracetranslator.TagError,
				VBool: true,
				VType: model.ValueType_BOOL,
			},
			{
				Key:   conventions.OtelStatusDescription,
				VType: model.ValueType_STRING,
				VStr:  "status-cancelled",
			},
		},
	}
}

func generateProtoSpanWithLibraryInfo(libraryName string) *model.Span {
	span := generateProtoSpan()
	span.Tags = append([]model.KeyValue{
		{
			Key:   conventions.InstrumentationLibraryName,
			VType: model.ValueType_STRING,
			VStr:  libraryName,
		}, {
			Key:   conventions.InstrumentationLibraryVersion,
			VType: model.ValueType_STRING,
			VStr:  "0.42.0",
		},
	}, span.Tags...)

	return span
}
func generateProtoSpanWithTraceState() *model.Span {
	return &model.Span{
		TraceID: model.NewTraceID(
			binary.BigEndian.Uint64([]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}),
			binary.BigEndian.Uint64([]byte{0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF, 0x80}),
		),
		SpanID:        model.NewSpanID(binary.BigEndian.Uint64([]byte{0xAF, 0xAE, 0xAD, 0xAC, 0xAB, 0xAA, 0xA9, 0xA8})),
		OperationName: "operationA",
		StartTime:     testSpanStartTime,
		Duration:      testSpanEndTime.Sub(testSpanStartTime),
		Logs: []model.Log{
			{
				Timestamp: testSpanEventTime,
				Fields: []model.KeyValue{
					{
						Key:   tracetranslator.TagMessage,
						VType: model.ValueType_STRING,
						VStr:  "event-with-attr",
					},
					{
						Key:   "span-event-attr",
						VType: model.ValueType_STRING,
						VStr:  "span-event-attr-val",
					},
				},
			},
			{
				Timestamp: testSpanEventTime,
				Fields: []model.KeyValue{
					{
						Key:    "attr-int",
						VType:  model.ValueType_INT64,
						VInt64: 123,
					},
				},
			},
		},
		Tags: []model.KeyValue{
			{
				Key:   tracetranslator.TagSpanKind,
				VType: model.ValueType_STRING,
				VStr:  string(tracetranslator.OpenTracingSpanKindClient),
			},
			{
				Key:   conventions.OtelStatusCode,
				VType: model.ValueType_STRING,
				VStr:  statusError,
			},
			{
				Key:   tracetranslator.TagError,
				VBool: true,
				VType: model.ValueType_BOOL,
			},
			{
				Key:   conventions.OtelStatusDescription,
				VType: model.ValueType_STRING,
				VStr:  "status-cancelled",
			},
			{
				Key:   tracetranslator.TagW3CTraceState,
				VType: model.ValueType_STRING,
				VStr:  "lasterror=f39cd56cc44274fd5abd07ef1164246d10ce2955",
			},
		},
	}
}

func generateTracesTwoSpansChildParent() pdata.Traces {
	td := generateTracesOneSpanNoResource()
	spans := td.ResourceSpans().At(0).InstrumentationLibrarySpans().At(0).Spans()

	span := spans.AppendEmpty()
	span.SetName("operationB")
	span.SetSpanID(pdata.NewSpanID([8]byte{0x1F, 0x1E, 0x1D, 0x1C, 0x1B, 0x1A, 0x19, 0x18}))
	span.SetParentSpanID(spans.At(0).SpanID())
	span.SetKind(pdata.SpanKindServer)
	span.SetTraceID(spans.At(0).TraceID())
	span.SetStartTimestamp(spans.At(0).StartTimestamp())
	span.SetEndTimestamp(spans.At(0).EndTimestamp())
	span.Status().SetCode(pdata.StatusCodeError)
	span.Attributes().InsertInt(conventions.AttributeHTTPStatusCode, 404)
	return td
}

func generateProtoChildSpan() *model.Span {
	traceID := model.NewTraceID(
		binary.BigEndian.Uint64([]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}),
		binary.BigEndian.Uint64([]byte{0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF, 0x80}),
	)
	return &model.Span{
		TraceID:       traceID,
		SpanID:        model.NewSpanID(binary.BigEndian.Uint64([]byte{0x1F, 0x1E, 0x1D, 0x1C, 0x1B, 0x1A, 0x19, 0x18})),
		OperationName: "operationB",
		StartTime:     testSpanStartTime,
		Duration:      testSpanEndTime.Sub(testSpanStartTime),
		Tags: []model.KeyValue{
			{
				Key:    conventions.AttributeHTTPStatusCode,
				VType:  model.ValueType_INT64,
				VInt64: 404,
			},
			{
				Key:   tracetranslator.TagSpanKind,
				VType: model.ValueType_STRING,
				VStr:  string(tracetranslator.OpenTracingSpanKindServer),
			},
		},
		References: []model.SpanRef{
			{
				TraceID: traceID,
				SpanID:  model.NewSpanID(binary.BigEndian.Uint64([]byte{0xAF, 0xAE, 0xAD, 0xAC, 0xAB, 0xAA, 0xA9, 0xA8})),
				RefType: model.SpanRefType_CHILD_OF,
			},
		},
	}
}

func generateTracesTwoSpansWithFollower() pdata.Traces {
	td := generateTracesOneSpanNoResource()
	spans := td.ResourceSpans().At(0).InstrumentationLibrarySpans().At(0).Spans()

	span := spans.AppendEmpty()
	span.SetName("operationC")
	span.SetSpanID(pdata.NewSpanID([8]byte{0x1F, 0x1E, 0x1D, 0x1C, 0x1B, 0x1A, 0x19, 0x18}))
	span.SetTraceID(spans.At(0).TraceID())
	span.SetStartTimestamp(spans.At(0).EndTimestamp())
	span.SetEndTimestamp(spans.At(0).EndTimestamp() + 1000000)
	span.SetKind(pdata.SpanKindConsumer)
	span.Status().SetCode(pdata.StatusCodeOk)
	span.Status().SetMessage("status-ok")
	link := span.Links().AppendEmpty()
	link.SetTraceID(span.TraceID())
	link.SetSpanID(spans.At(0).SpanID())
	return td
}

func generateProtoFollowerSpan() *model.Span {
	traceID := model.NewTraceID(
		binary.BigEndian.Uint64([]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}),
		binary.BigEndian.Uint64([]byte{0xF9, 0xFA, 0xFB, 0xFC, 0xFD, 0xFE, 0xFF, 0x80}),
	)
	return &model.Span{
		TraceID:       traceID,
		SpanID:        model.NewSpanID(binary.BigEndian.Uint64([]byte{0x1F, 0x1E, 0x1D, 0x1C, 0x1B, 0x1A, 0x19, 0x18})),
		OperationName: "operationC",
		StartTime:     testSpanEndTime,
		Duration:      time.Millisecond,
		Tags: []model.KeyValue{
			{
				Key:   tracetranslator.TagSpanKind,
				VType: model.ValueType_STRING,
				VStr:  string(tracetranslator.OpenTracingSpanKindConsumer),
			},
			{
				Key:   conventions.OtelStatusCode,
				VType: model.ValueType_STRING,
				VStr:  statusOk,
			},
			{
				Key:   conventions.OtelStatusDescription,
				VType: model.ValueType_STRING,
				VStr:  "status-ok",
			},
		},
		References: []model.SpanRef{
			{
				TraceID: traceID,
				SpanID:  model.NewSpanID(binary.BigEndian.Uint64([]byte{0xAF, 0xAE, 0xAD, 0xAC, 0xAB, 0xAA, 0xA9, 0xA8})),
				RefType: model.SpanRefType_FOLLOWS_FROM,
			},
		},
	}
}

func BenchmarkProtoBatchToInternalTraces(b *testing.B) {
	jb := []*model.Batch{
		{
			Process: generateProtoProcess(),
			Spans: []*model.Span{
				generateProtoSpan(),
				generateProtoChildSpan(),
			},
		}}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := ProtoToTraces(jb)
		assert.NoError(b, err)
	}
}

func generateTracesTwoSpansFromTwoLibraries() pdata.Traces {
	td := testdata.GenerateTracesOneEmptyResourceSpans()

	rs0 := td.ResourceSpans().At(0)
	rs0.InstrumentationLibrarySpans().EnsureCapacity(2)

	rs0ils0 := rs0.InstrumentationLibrarySpans().AppendEmpty()
	rs0ils0.InstrumentationLibrary().SetName("library1")
	rs0ils0.InstrumentationLibrary().SetVersion("0.42.0")
	span1 := rs0ils0.Spans().AppendEmpty()
	span1.SetTraceID(idutils.UInt64ToTraceID(0, 0))
	span1.SetSpanID(idutils.UInt64ToSpanID(0))
	span1.SetName("operation1")
	span1.SetStartTimestamp(testSpanStartTimestamp)
	span1.SetEndTimestamp(testSpanEndTimestamp)

	rs0ils1 := rs0.InstrumentationLibrarySpans().AppendEmpty()
	rs0ils1.InstrumentationLibrary().SetName("library2")
	rs0ils1.InstrumentationLibrary().SetVersion("0.42.0")
	span2 := rs0ils1.Spans().AppendEmpty()
	span2.SetTraceID(span1.TraceID())
	span2.SetSpanID(span1.SpanID())
	span2.SetName("operation2")
	span2.SetStartTimestamp(testSpanStartTimestamp)
	span2.SetEndTimestamp(testSpanEndTimestamp)

	return td
}
