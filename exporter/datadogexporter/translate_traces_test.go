// Copyright The OpenTelemetry Authors
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

package datadogexporter

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/DataDog/datadog-agent/pkg/trace/exportable/obfuscate"
	"github.com/DataDog/datadog-agent/pkg/trace/exportable/pb"
	"github.com/DataDog/datadog-agent/pkg/trace/exportable/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/model/pdata"
	conventions "go.opentelemetry.io/collector/model/semconv/v1.5.0"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/internal/model/attributes"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/internal/testutils"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/internal/utils"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func NewResourceSpansData(mockTraceID [16]byte, mockSpanID [8]byte, mockParentSpanID [8]byte, statusCode pdata.StatusCode, resourceEnvAndService bool, endTime time.Time) pdata.ResourceSpans {
	// The goal of this test is to ensure that each span in
	// pdata.ResourceSpans is transformed to its *trace.SpanData correctly!

	pdataEndTime := pdata.NewTimestampFromTime(endTime)
	startTime := endTime.Add(-90 * time.Second)
	pdataStartTime := pdata.NewTimestampFromTime(startTime)

	rs := pdata.NewResourceSpans()
	ilss := rs.InstrumentationLibrarySpans()
	ils := ilss.AppendEmpty()
	ils.InstrumentationLibrary().SetName("test_il_name")
	ils.InstrumentationLibrary().SetVersion("test_il_version")
	span := ils.Spans().AppendEmpty()

	traceID := pdata.NewTraceID(mockTraceID)
	spanID := pdata.NewSpanID(mockSpanID)
	parentSpanID := pdata.NewSpanID(mockParentSpanID)
	span.SetTraceID(traceID)
	span.SetSpanID(spanID)
	span.SetParentSpanID(parentSpanID)
	span.SetName("End-To-End Here")
	span.SetKind(pdata.SpanKindServer)
	span.SetStartTimestamp(pdataStartTime)
	span.SetEndTimestamp(pdataEndTime)
	span.SetTraceState("tracestatekey=tracestatevalue")

	status := span.Status()
	if statusCode == pdata.StatusCodeError {
		status.SetCode(pdata.StatusCodeError)
		status.SetMessage("This is not a drill!")
	} else {
		status.SetCode(statusCode)
	}

	events := span.Events()
	events.EnsureCapacity(2)

	evt := events.AppendEmpty()
	evt.SetTimestamp(pdataStartTime)
	evt.SetName("start")

	evt = events.AppendEmpty()
	evt.SetTimestamp(pdataEndTime)
	evt.SetName("end")
	evt.Attributes().InsertBool("flag", false)
	attribs := map[string]pdata.AttributeValue{
		"cache_hit":  pdata.NewAttributeValueBool(true),
		"timeout_ns": pdata.NewAttributeValueInt(12e9),
		"ping_count": pdata.NewAttributeValueInt(25),
		"agent":      pdata.NewAttributeValueString("ocagent"),
	}

	if statusCode == pdata.StatusCodeError {
		attribs["http.status_code"] = pdata.NewAttributeValueString("501")
	}

	pdata.NewAttributeMapFromMap(attribs).CopyTo(span.Attributes())

	resource := rs.Resource()

	if resourceEnvAndService {
		resource.Attributes().InsertString(conventions.AttributeContainerID, "3249847017410247")
		resource.Attributes().InsertString(conventions.AttributeDeploymentEnvironment, "test-env")
		resource.Attributes().InsertString(conventions.AttributeK8SPodName, "example-pod-name")
		resource.Attributes().InsertString(conventions.AttributeAWSECSTaskARN, "arn:aws:ecs:ap-southwest-1:241423265983:task/test-environment-test-echo-Cluster-2lrqTJKFjACT/746bf64740324812835f688c30cf1512")
		resource.Attributes().InsertString("namespace", "kube-system")
		resource.Attributes().InsertString("service.name", "test-resource-service-name")
		resource.Attributes().InsertString("service.version", "test-version")
	} else {
		resource.Attributes().InsertString("namespace", "kube-system")
	}

	return rs
}

func TestConvertToDatadogTd(t *testing.T) {
	traces := pdata.NewTraces()
	traces.ResourceSpans().AppendEmpty()
	denylister := newDenylister([]string{})
	buildInfo := component.BuildInfo{
		Version: "1.0",
	}

	outputTraces, runningMetrics := convertToDatadogTd(traces, "test-host", &config.Config{}, denylister, buildInfo)

	assert.Equal(t, 1, len(outputTraces))
	assert.Equal(t, 1, len(runningMetrics))
}

func TestConvertToDatadogTdNoResourceSpans(t *testing.T) {
	traces := pdata.NewTraces()
	denylister := newDenylister([]string{})
	buildInfo := component.BuildInfo{
		Version: "1.0",
	}

	outputTraces, runningMetrics := convertToDatadogTd(traces, "test-host", &config.Config{}, denylister, buildInfo)

	assert.Equal(t, 0, len(outputTraces))
	assert.Equal(t, 0, len(runningMetrics))
}

func TestRunningTraces(t *testing.T) {
	td := pdata.NewTraces()

	rts := td.ResourceSpans()

	rt := rts.AppendEmpty()
	resAttrs := rt.Resource().Attributes()
	resAttrs.Insert(attributes.AttributeDatadogHostname, pdata.NewAttributeValueString("resource-hostname-1"))

	rt = rts.AppendEmpty()
	resAttrs = rt.Resource().Attributes()
	resAttrs.Insert(attributes.AttributeDatadogHostname, pdata.NewAttributeValueString("resource-hostname-1"))

	rt = rts.AppendEmpty()
	resAttrs = rt.Resource().Attributes()
	resAttrs.Insert(attributes.AttributeDatadogHostname, pdata.NewAttributeValueString("resource-hostname-2"))

	rts.AppendEmpty()

	buildInfo := component.BuildInfo{
		Version: "1.0",
	}

	_, runningMetrics := convertToDatadogTd(td, "fallbackHost", &config.Config{}, newDenylister([]string{}), buildInfo)

	runningHostnames := []string{}
	for _, metric := range runningMetrics {
		require.Equal(t, *metric.Metric, "otel.datadog_exporter.traces.running")
		require.NotNil(t, metric.Host)
		runningHostnames = append(runningHostnames, *metric.Host)
	}

	assert.ElementsMatch(t,
		runningHostnames,
		[]string{"resource-hostname-1", "resource-hostname-2", "fallbackHost"},
	)
}

func TestRunningTracesARN(t *testing.T) {
	td := pdata.NewTraces()
	rts := td.ResourceSpans()

	rm := rts.AppendEmpty()
	baseAttrs := testutils.NewAttributeMap(map[string]string{
		conventions.AttributeCloudProvider:      conventions.AttributeCloudProviderAWS,
		conventions.AttributeCloudPlatform:      conventions.AttributeCloudPlatformAWSECS,
		conventions.AttributeAWSECSTaskFamily:   "example-task-family",
		conventions.AttributeAWSECSTaskRevision: "example-task-revision",
		conventions.AttributeAWSECSLaunchtype:   conventions.AttributeAWSECSLaunchtypeFargate,
	})
	baseAttrs.CopyTo(rm.Resource().Attributes())
	rm.Resource().Attributes().InsertString(conventions.AttributeAWSECSTaskARN, "task-arn-1")

	rm = rts.AppendEmpty()
	baseAttrs.CopyTo(rm.Resource().Attributes())
	rm.Resource().Attributes().InsertString(conventions.AttributeAWSECSTaskARN, "task-arn-2")

	rm = rts.AppendEmpty()
	baseAttrs.CopyTo(rm.Resource().Attributes())
	rm.Resource().Attributes().InsertString(conventions.AttributeAWSECSTaskARN, "task-arn-3")

	buildInfo := component.BuildInfo{}

	_, runningMetrics := convertToDatadogTd(td, "fallbackHost", &config.Config{}, newDenylister([]string{}), buildInfo)

	runningHostnames := []string{}
	runningTags := []string{}
	for _, metric := range runningMetrics {
		require.Equal(t, *metric.Metric, "otel.datadog_exporter.traces.running")
		require.NotNil(t, metric.Host)
		runningHostnames = append(runningHostnames, *metric.Host)
		runningTags = append(runningTags, metric.Tags...)
	}

	assert.ElementsMatch(t, runningHostnames, []string{"", "", ""})
	assert.Len(t, runningMetrics, 3)
	assert.ElementsMatch(t, runningTags, []string{"task_arn:task-arn-1", "task_arn:task-arn-2", "task_arn:task-arn-3"})
}

func TestObfuscation(t *testing.T) {

	denylister := newDenylister([]string{})
	buildInfo := component.BuildInfo{
		Version: "1.0",
	}

	traces := pdata.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	resource := rs.Resource()
	resource.Attributes().InsertString("service.name", "sure")
	ilss := rs.InstrumentationLibrarySpans().AppendEmpty()
	instrumentationLibrary := ilss.InstrumentationLibrary()
	instrumentationLibrary.SetName("flash")
	instrumentationLibrary.SetVersion("v1")

	span := ilss.Spans().AppendEmpty()

	// Make this a FaaS span, which will trigger an error, because conversion
	// of them is currently not supported.
	span.Attributes().InsertString("testinfo?=123", "http.route")

	outputTraces, _ := convertToDatadogTd(traces, "test-host", &config.Config{}, denylister, buildInfo)

	aggregatedTraces := aggregateTracePayloadsByEnv(outputTraces)

	obfuscator := obfuscate.NewObfuscator(obfuscatorConfig)

	obfuscatePayload(obfuscator, aggregatedTraces)

	assert.Equal(t, 1, len(aggregatedTraces))
}

func TestBasicTracesTranslation(t *testing.T) {
	hostname := "testhostname"
	denylister := newDenylister([]string{})

	// generate mock trace, span and parent span ids
	mockTraceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	mockSpanID := [8]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}
	mockParentSpanID := [8]byte{0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8}
	mockEndTime := time.Now().Round(time.Second)

	// create mock resource span data
	// set shouldError and resourceServiceandEnv to false to test defaut behavior
	rs := NewResourceSpansData(mockTraceID, mockSpanID, mockParentSpanID, pdata.StatusCodeUnset, false, mockEndTime)

	// translate mocks to datadog traces
	datadogPayload := resourceSpansToDatadogSpans(rs, hostname, &config.Config{}, denylister, map[string]string{})

	// ensure we return the correct type
	assert.IsType(t, pb.TracePayload{}, datadogPayload)

	// ensure hostname arg is respected
	assert.Equal(t, hostname, datadogPayload.HostName)
	assert.Equal(t, 1, len(datadogPayload.Traces))

	// ensure trace id gets translated to uint64 correctly
	assert.Equal(t, decodeAPMTraceID(mockTraceID), datadogPayload.Traces[0].TraceID)

	// ensure the correct number of spans are expected
	assert.Equal(t, 1, len(datadogPayload.Traces[0].Spans))

	// ensure span's trace id matches payload trace id
	assert.Equal(t, datadogPayload.Traces[0].TraceID, datadogPayload.Traces[0].Spans[0].TraceID)

	// ensure span's spanId and parentSpanId are set correctly
	assert.Equal(t, decodeAPMSpanID(mockSpanID), datadogPayload.Traces[0].Spans[0].SpanID)
	assert.Equal(t, decodeAPMSpanID(mockParentSpanID), datadogPayload.Traces[0].Spans[0].ParentID)

	// ensure original TraceID is preserved
	assert.Equal(t, pdata.NewTraceID(mockTraceID).HexString(), datadogPayload.Traces[0].Spans[0].Meta["otel.trace_id"])

	// ensure that span.resource defaults to otlp span.name
	assert.Equal(t, "End-To-End Here", datadogPayload.Traces[0].Spans[0].Resource)

	// ensure that span.name defaults to string representing instrumentation library if present
	assert.Equal(t, strings.ToLower(fmt.Sprintf("%s.%s", datadogPayload.Traces[0].Spans[0].Meta[conventions.InstrumentationLibraryName], strings.TrimPrefix(pdata.SpanKindServer.String(), "SPAN_KIND_"))), datadogPayload.Traces[0].Spans[0].Name)

	// ensure that span.type is based on otlp span.kind
	assert.Equal(t, "web", datadogPayload.Traces[0].Spans[0].Type)

	// ensure that span.meta and span.metrics pick up attributes, instrumentation ibrary and resource attribs
	assert.Equal(t, 11, len(datadogPayload.Traces[0].Spans[0].Meta))
	assert.Equal(t, 1, len(datadogPayload.Traces[0].Spans[0].Metrics))

	// ensure that span error is based on otlp span status
	assert.Equal(t, int32(0), datadogPayload.Traces[0].Spans[0].Error)

	// ensure that span meta also inccludes correctly sets resource attributes
	assert.Equal(t, "kube-system", datadogPayload.Traces[0].Spans[0].Meta["namespace"])

	// ensure that span service name gives resource service.name priority
	assert.Equal(t, "otlpresourcenoservicename", datadogPayload.Traces[0].Spans[0].Service)

	// ensure a duration and start time are calculated
	assert.NotNil(t, datadogPayload.Traces[0].Spans[0].Start)
	assert.NotNil(t, datadogPayload.Traces[0].Spans[0].Duration)

	pdataMockEndTime := pdata.NewTimestampFromTime(mockEndTime)
	pdataMockStartTime := pdata.NewTimestampFromTime(mockEndTime.Add(-90 * time.Second))
	mockEventsString := fmt.Sprintf("[{\"attributes\":{},\"name\":\"start\",\"time\":%d},{\"attributes\":{\"flag\":false},\"name\":\"end\",\"time\":%d}]", pdataMockStartTime, pdataMockEndTime)

	// ensure that events tag is set if span events exist and contains structured json fields
	assert.Equal(t, mockEventsString, datadogPayload.Traces[0].Spans[0].Meta["events"])
}

func TestBasicTracesDenylist(t *testing.T) {
	hostname := "testhostname"

	// adding some regex bits to the resource name, but this should drop the trace
	denylister := newDenylister([]string{".nd-To-E.d H.re"})

	// generate mock trace, span and parent span ids
	mockTraceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	mockSpanID := [8]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}
	mockParentSpanID := [8]byte{0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8}
	mockEndTime := time.Now().Round(time.Second)

	// create mock resource span data
	// set shouldError and resourceServiceandEnv to false to test defaut behavior
	rs := NewResourceSpansData(mockTraceID, mockSpanID, mockParentSpanID, pdata.StatusCodeUnset, false, mockEndTime)

	// translate mocks to datadog traces
	datadogPayload := resourceSpansToDatadogSpans(rs, hostname, &config.Config{}, denylister, map[string]string{})

	// ensure we return the correct type
	assert.IsType(t, pb.TracePayload{}, datadogPayload)

	// ensure hostname arg is respected
	assert.Equal(t, hostname, datadogPayload.HostName)
	assert.Equal(t, 0, len(datadogPayload.Traces))
}

func TestTracesTranslationErrorsAndResource(t *testing.T) {
	hostname := "testhostname"
	denylister := newDenylister([]string{})

	// generate mock trace, span and parent span ids
	mockTraceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	mockSpanID := [8]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}
	mockParentSpanID := [8]byte{0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8}

	mockEndTime := time.Now().Round(time.Second)

	// create mock resource span data
	// toggle on errors and custom service naming to test edge case code paths
	rs := NewResourceSpansData(mockTraceID, mockSpanID, mockParentSpanID, pdata.StatusCodeError, true, mockEndTime)

	// translate mocks to datadog traces
	cfg := config.Config{
		TagsConfig: config.TagsConfig{
			Version: "v1",
		},
	}

	datadogPayload := resourceSpansToDatadogSpans(rs, hostname, &cfg, denylister, map[string]string{})

	// ensure we return the correct type
	assert.IsType(t, pb.TracePayload{}, datadogPayload)

	// ensure hostname arg is respected
	assert.Equal(t, hostname, datadogPayload.HostName)
	assert.Equal(t, 1, len(datadogPayload.Traces))

	// ensure that span error is based on otlp span status
	assert.Equal(t, int32(1), datadogPayload.Traces[0].Spans[0].Error)

	// ensure that span status code is set
	assert.Equal(t, "501", datadogPayload.Traces[0].Spans[0].Meta["http.status_code"])

	// ensure that span service name gives resource service.name priority
	assert.Equal(t, "test-resource-service-name", datadogPayload.Traces[0].Spans[0].Service)

	// ensure that env gives resource deployment.environment priority
	assert.Equal(t, "test-env", datadogPayload.Env)

	// ensure that version gives resource service.version priority
	assert.Equal(t, "test-version", datadogPayload.Traces[0].Spans[0].Meta["version"])

	assert.Equal(t, 21, len(datadogPayload.Traces[0].Spans[0].Meta))

	assert.Contains(t, datadogPayload.Traces[0].Spans[0].Meta[tagContainersTags], "container_id:3249847017410247")
	assert.Contains(t, datadogPayload.Traces[0].Spans[0].Meta[tagContainersTags], "pod_name:example-pod-name")
	assert.Contains(t, datadogPayload.Traces[0].Spans[0].Meta[tagContainersTags], "arn:aws:ecs:ap-southwest-1:241423265983:task/test-environment-test-echo-Cluster-2lrqTJKFjACT/746bf64740324812835f688c30cf1512")

	// ensure that span error type uses a fallback of "error" if a status code exists
	assert.Equal(t, "error", datadogPayload.Traces[0].Spans[0].Meta["error.type"])
}

func TestTracesFallbackErrorMessage(t *testing.T) {
	hostname := "testhostname"
	denylister := newDenylister([]string{})

	// generate mock trace, span and parent span ids
	mockTraceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	mockSpanID := [8]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}
	mockParentSpanID := [8]byte{0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8}
	mockEndTime := time.Now().Round(time.Second)
	pdataEndTime := pdata.NewTimestampFromTime(mockEndTime)
	startTime := mockEndTime.Add(-90 * time.Second)
	pdataStartTime := pdata.NewTimestampFromTime(startTime)

	rs := pdata.NewResourceSpans()
	ilss := rs.InstrumentationLibrarySpans()
	ils := ilss.AppendEmpty()
	ils.InstrumentationLibrary().SetName("test_il_name")
	ils.InstrumentationLibrary().SetVersion("test_il_version")
	span := ils.Spans().AppendEmpty()

	traceID := pdata.NewTraceID(mockTraceID)
	spanID := pdata.NewSpanID(mockSpanID)
	parentSpanID := pdata.NewSpanID(mockParentSpanID)
	span.SetTraceID(traceID)
	span.SetSpanID(spanID)
	span.SetParentSpanID(parentSpanID)
	span.SetName("End-To-End Here")
	span.SetKind(pdata.SpanKindServer)
	span.SetStartTimestamp(pdataStartTime)
	span.SetEndTimestamp(pdataEndTime)
	span.SetTraceState("tracestatekey=tracestatevalue")

	status := span.Status()
	status.SetCode(pdata.StatusCodeError)

	span.Attributes().InsertString(conventions.AttributeHTTPStatusCode, "404")
	span.Attributes().InsertString("http.status_text", "Not Found")

	// translate mocks to datadog traces
	datadogPayload := resourceSpansToDatadogSpans(rs, hostname, &config.Config{}, denylister, map[string]string{})

	// ensure that span error type uses a fallback of "error"
	assert.Equal(t, "error", datadogPayload.Traces[0].Spans[0].Meta["error.type"])

	// ensure that span error type uses a fallback of "status_code" and "status_text"
	assert.Equal(t, "404 Not Found", datadogPayload.Traces[0].Spans[0].Meta["error.msg"])
}

// Ensures that if more than one error event occurs in a span, the last one is used for translation
func TestTracesTranslationErrorsFromEventsUsesLast(t *testing.T) {
	hostname := "testhostname"
	denylister := newDenylister([]string{})

	// generate mock trace, span and parent span ids
	mockTraceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	mockSpanID := [8]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}
	mockParentSpanID := [8]byte{0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8}

	attribs := map[string]pdata.AttributeValue{
		conventions.AttributeExceptionType:       pdata.NewAttributeValueString("HttpError"),
		conventions.AttributeExceptionStacktrace: pdata.NewAttributeValueString("HttpError at line 67\nthing at line 45"),
		conventions.AttributeExceptionMessage:    pdata.NewAttributeValueString("HttpError error occurred"),
	}

	mockEndTime := time.Now().Round(time.Second)

	// create mock resource span data
	// toggle on errors and custom service naming to test edge case code paths
	rs := NewResourceSpansData(mockTraceID, mockSpanID, mockParentSpanID, pdata.StatusCodeError, true, mockEndTime)
	span := rs.InstrumentationLibrarySpans().At(0).Spans().At(0)
	events := span.Events()

	events.AppendEmpty().SetName("start")

	event := events.AppendEmpty()
	event.SetName(AttributeExceptionEventName)
	event.Attributes().InsertString(conventions.AttributeExceptionType, "SomeOtherErr")
	event.Attributes().InsertString(conventions.AttributeExceptionStacktrace, "SomeOtherErr at line 67\nthing at line 45")
	event.Attributes().InsertString(conventions.AttributeExceptionMessage, "SomeOtherErr error occurred")

	event = events.AppendEmpty()
	event.SetName(AttributeExceptionEventName)
	pdata.NewAttributeMapFromMap(attribs).CopyTo(event.Attributes())

	event = events.AppendEmpty()
	event.SetName("end")
	event.Attributes().InsertBool("flag", false)

	// translate mocks to datadog traces
	cfg := config.Config{
		TagsConfig: config.TagsConfig{
			Version: "v1",
		},
	}

	datadogPayload := resourceSpansToDatadogSpans(rs, hostname, &cfg, denylister, map[string]string{})

	// Ensure the error type is copied over from the last error event logged
	assert.Equal(t, attribs[conventions.AttributeExceptionType].StringVal(), datadogPayload.Traces[0].Spans[0].Meta[ext.ErrorType])

	// Ensure the stack trace is copied over from the last error event logged
	assert.Equal(t, attribs[conventions.AttributeExceptionStacktrace].StringVal(), datadogPayload.Traces[0].Spans[0].Meta[ext.ErrorStack])

	// Ensure the error message is copied over from the last error event logged
	assert.Equal(t, attribs[conventions.AttributeExceptionMessage].StringVal(), datadogPayload.Traces[0].Spans[0].Meta[ext.ErrorMsg])
}

// Ensures that if the first or last event in the list is the error, that translation still behaves properly
func TestTracesTranslationErrorsFromEventsBounds(t *testing.T) {
	hostname := "testhostname"
	denylister := newDenylister([]string{})

	// generate mock trace, span and parent span ids
	mockTraceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	mockSpanID := [8]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}
	mockParentSpanID := [8]byte{0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8}

	mockEndTime := time.Now().Round(time.Second)

	// create mock resource span data
	// toggle on errors and custom service naming to test edge case code paths
	rs := NewResourceSpansData(mockTraceID, mockSpanID, mockParentSpanID, pdata.StatusCodeError, true, mockEndTime)
	span := rs.InstrumentationLibrarySpans().At(0).Spans().At(0)
	events := span.Events()
	events.EnsureCapacity(3)

	// Start with the error as the first element in the list...
	attribs := map[string]pdata.AttributeValue{
		conventions.AttributeExceptionType:       pdata.NewAttributeValueString("HttpError"),
		conventions.AttributeExceptionStacktrace: pdata.NewAttributeValueString("HttpError at line 67\nthing at line 45"),
		conventions.AttributeExceptionMessage:    pdata.NewAttributeValueString("HttpError error occurred"),
	}

	evt := events.AppendEmpty()
	evt.SetName(AttributeExceptionEventName)
	pdata.NewAttributeMapFromMap(attribs).CopyTo(evt.Attributes())

	evt = events.AppendEmpty()
	evt.SetName("start")

	evt = events.AppendEmpty()
	evt.SetName("end")
	evt.Attributes().InsertBool("flag", false)

	// translate mocks to datadog traces
	cfg := config.Config{
		TagsConfig: config.TagsConfig{
			Version: "v1",
		},
	}

	datadogPayload := resourceSpansToDatadogSpans(rs, hostname, &cfg, denylister, map[string]string{})

	// Ensure the error type is copied over
	assert.Equal(t, attribs[conventions.AttributeExceptionType].StringVal(), datadogPayload.Traces[0].Spans[0].Meta[ext.ErrorType])

	// Ensure the stack trace is copied over
	assert.Equal(t, attribs[conventions.AttributeExceptionStacktrace].StringVal(), datadogPayload.Traces[0].Spans[0].Meta[ext.ErrorStack])

	// Ensure the error message is copied over
	assert.Equal(t, attribs[conventions.AttributeExceptionMessage].StringVal(), datadogPayload.Traces[0].Spans[0].Meta[ext.ErrorMsg])

	// Now with the error event at the end of the list...
	events.At(0).SetName("start")
	// Reset the attributes
	pdata.NewAttributeMap().CopyTo(events.At(0).Attributes())

	events.At(1).SetName("end")
	events.At(1).Attributes().InsertBool("flag", false)

	events.At(2).SetName(AttributeExceptionEventName)
	pdata.NewAttributeMapFromMap(attribs).CopyTo(events.At(2).Attributes())

	// Ensure the error type is copied over
	assert.Equal(t, attribs[conventions.AttributeExceptionType].StringVal(), datadogPayload.Traces[0].Spans[0].Meta[ext.ErrorType])

	// Ensure the stack trace is copied over
	assert.Equal(t, attribs[conventions.AttributeExceptionStacktrace].StringVal(), datadogPayload.Traces[0].Spans[0].Meta[ext.ErrorStack])

	// Ensure the error message is copied over
	assert.Equal(t, attribs[conventions.AttributeExceptionMessage].StringVal(), datadogPayload.Traces[0].Spans[0].Meta[ext.ErrorMsg])
}

func TestTracesTranslationOkStatus(t *testing.T) {
	hostname := "testhostname"
	denylister := newDenylister([]string{})

	// generate mock trace, span and parent span ids
	mockTraceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	mockSpanID := [8]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}
	mockParentSpanID := [8]byte{0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8}

	mockEndTime := time.Now().Round(time.Second)

	// create mock resource span data
	// toggle on errors and custom service naming to test edge case code paths
	rs := NewResourceSpansData(mockTraceID, mockSpanID, mockParentSpanID, pdata.StatusCodeError, true, mockEndTime)

	// translate mocks to datadog traces
	cfg := config.Config{
		TagsConfig: config.TagsConfig{
			Version: "v1",
		},
	}

	datadogPayload := resourceSpansToDatadogSpans(rs, hostname, &cfg, denylister, map[string]string{})

	// ensure we return the correct type
	assert.IsType(t, pb.TracePayload{}, datadogPayload)

	// ensure hostname arg is respected
	assert.Equal(t, hostname, datadogPayload.HostName)
	assert.Equal(t, 1, len(datadogPayload.Traces))

	// ensure that span error is based on otlp span status
	assert.Equal(t, int32(1), datadogPayload.Traces[0].Spans[0].Error)

	// ensure that span status code is set
	assert.Equal(t, "501", datadogPayload.Traces[0].Spans[0].Meta["http.status_code"])

	// ensure that span service name gives resource service.name priority
	assert.Equal(t, "test-resource-service-name", datadogPayload.Traces[0].Spans[0].Service)

	// ensure that env gives resource deployment.environment priority
	assert.Equal(t, "test-env", datadogPayload.Env)

	// ensure that version gives resource service.version priority
	assert.Equal(t, "test-version", datadogPayload.Traces[0].Spans[0].Meta["version"])

	assert.Equal(t, 21, len(datadogPayload.Traces[0].Spans[0].Meta))
}

// ensure that the datadog span uses the configured unified service tags
func TestTracesTranslationConfig(t *testing.T) {
	hostname := "testhostname"
	denylister := newDenylister([]string{})

	// generate mock trace, span and parent span ids
	mockTraceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	mockSpanID := [8]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}
	mockParentSpanID := [8]byte{0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8}

	mockEndTime := time.Now().Round(time.Second)

	// create mock resource span data
	// toggle on errors and custom service naming to test edge case code paths
	rs := NewResourceSpansData(mockTraceID, mockSpanID, mockParentSpanID, pdata.StatusCodeUnset, true, mockEndTime)

	cfg := config.Config{
		TagsConfig: config.TagsConfig{
			Version: "v1",
			Service: "alt-service",
		},
	}

	// translate mocks to datadog traces
	datadogPayload := resourceSpansToDatadogSpans(rs, hostname, &cfg, denylister, map[string]string{})
	// ensure we return the correct type
	assert.IsType(t, pb.TracePayload{}, datadogPayload)

	// ensure hostname arg is respected
	assert.Equal(t, hostname, datadogPayload.HostName)
	assert.Equal(t, 1, len(datadogPayload.Traces))

	// ensure that span error is based on otlp span status, unset should not error
	assert.Equal(t, int32(0), datadogPayload.Traces[0].Spans[0].Error)

	// ensure that span service name gives resource service.name priority
	assert.Equal(t, "test-resource-service-name", datadogPayload.Traces[0].Spans[0].Service)

	// ensure that env gives resource deployment.environment priority
	assert.Equal(t, "test-env", datadogPayload.Env)

	// ensure that version gives resource service.version priority
	assert.Equal(t, "test-version", datadogPayload.Traces[0].Spans[0].Meta["version"])

	assert.Equal(t, 18, len(datadogPayload.Traces[0].Spans[0].Meta))
}

// ensure that the translation returns early if no resource instrumentation library spans
func TestTracesTranslationNoIls(t *testing.T) {
	hostname := "testhostname"
	denylister := newDenylister([]string{})

	rs := pdata.NewResourceSpans()

	cfg := config.Config{
		TagsConfig: config.TagsConfig{
			Version: "v1",
			Service: "alt-service",
		},
	}

	// translate mocks to datadog traces
	datadogPayload := resourceSpansToDatadogSpans(rs, hostname, &cfg, denylister, map[string]string{})
	// ensure we return the correct type
	assert.IsType(t, pb.TracePayload{}, datadogPayload)

	// ensure hostname arg is respected
	assert.Equal(t, hostname, datadogPayload.HostName)
	assert.Equal(t, 0, len(datadogPayload.Traces))
}

// ensure that the translation returns early if no resource instrumentation library spans
func TestTracesTranslationInvalidService(t *testing.T) {
	hostname := "testhostname"
	denylister := newDenylister([]string{})

	// generate mock trace, span and parent span ids
	mockTraceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	mockSpanID := [8]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}
	mockParentSpanID := [8]byte{0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8}

	mockEndTime := time.Now().Round(time.Second)

	// create mock resource span data
	// toggle on errors and custom service naming to test edge case code paths
	rs := NewResourceSpansData(mockTraceID, mockSpanID, mockParentSpanID, pdata.StatusCodeUnset, false, mockEndTime)

	// add a tab and an invalid character to see if it gets normalized
	cfgInvalidService := config.Config{
		TagsConfig: config.TagsConfig{
			Version: "v1",
			Service: "alt-s	ervice",
		},
	}

	// use only an invalid character
	cfgEmptyService := config.Config{
		TagsConfig: config.TagsConfig{
			Version: "v1",
			Service: "	",
		},
	}

	// start with an invalid character
	cfgStartWithInvalidService := config.Config{
		TagsConfig: config.TagsConfig{
			Version: "v1",
			Service: "	alt-service",
		},
	}

	// translate mocks to datadog traces
	datadogPayloadInvalidService := resourceSpansToDatadogSpans(rs, hostname, &cfgInvalidService, denylister, map[string]string{})
	datadogPayloadEmptyService := resourceSpansToDatadogSpans(rs, hostname, &cfgEmptyService, denylister, map[string]string{})
	datadogPayloadStartWithInvalidService := resourceSpansToDatadogSpans(rs, hostname, &cfgStartWithInvalidService, denylister, map[string]string{})

	// ensure we return the correct type
	assert.IsType(t, pb.TracePayload{}, datadogPayloadInvalidService)
	assert.IsType(t, pb.TracePayload{}, datadogPayloadEmptyService)
	assert.IsType(t, pb.TracePayload{}, datadogPayloadStartWithInvalidService)

	// ensure that span service name replaces invalid chars
	assert.Equal(t, "alt-s_ervice", datadogPayloadInvalidService.Traces[0].Spans[0].Service)
	// ensure that span service name has default
	assert.Equal(t, "unnamed-otel-service", datadogPayloadEmptyService.Traces[0].Spans[0].Service)
	// ensure that span service name removes invalid starting chars
	assert.Equal(t, "alt-service", datadogPayloadStartWithInvalidService.Traces[0].Spans[0].Service)
}

// ensure that the datadog span uses the peer.name instead service.name when provided
func TestTracesTranslationServicePeerName(t *testing.T) {
	hostname := "testhostname"
	denylister := newDenylister([]string{})

	// generate mock trace, span and parent span ids
	mockTraceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	mockSpanID := [8]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}
	mockParentSpanID := [8]byte{0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8}
	mockEndTime := time.Now().Round(time.Second)

	// create mock resource span data
	// set shouldError and resourceServiceandEnv to false to test defaut behavior
	rs := NewResourceSpansData(mockTraceID, mockSpanID, mockParentSpanID, pdata.StatusCodeUnset, false, mockEndTime)

	span := rs.InstrumentationLibrarySpans().At(0).Spans().At(0)
	span.Attributes().InsertString(conventions.AttributePeerService, "my_peer_service_name")

	// translate mocks to datadog traces
	datadogPayload := resourceSpansToDatadogSpans(rs, hostname, &config.Config{}, denylister, map[string]string{})
	// ensure we return the correct type
	assert.IsType(t, pb.TracePayload{}, datadogPayload)

	// ensure hostname arg is respected
	assert.Equal(t, hostname, datadogPayload.HostName)
	assert.Equal(t, 1, len(datadogPayload.Traces))

	// ensure trace id gets translated to uint64 correctly
	assert.Equal(t, decodeAPMTraceID(mockTraceID), datadogPayload.Traces[0].TraceID)

	// ensure the correct number of spans are expected
	assert.Equal(t, 1, len(datadogPayload.Traces[0].Spans))

	// ensure span's trace id matches payload trace id
	assert.Equal(t, datadogPayload.Traces[0].TraceID, datadogPayload.Traces[0].Spans[0].TraceID)

	// ensure span's spanId and parentSpanId are set correctly
	assert.Equal(t, decodeAPMSpanID(mockSpanID), datadogPayload.Traces[0].Spans[0].SpanID)
	assert.Equal(t, decodeAPMSpanID(mockParentSpanID), datadogPayload.Traces[0].Spans[0].ParentID)

	// ensure that span.resource defaults to otlp span.name
	assert.Equal(t, "End-To-End Here", datadogPayload.Traces[0].Spans[0].Resource)

	// ensure that span.name defaults to string representing instrumentation library if present
	assert.Equal(t, strings.ToLower(fmt.Sprintf("%s.%s", datadogPayload.Traces[0].Spans[0].Meta[conventions.InstrumentationLibraryName], strings.TrimPrefix(pdata.SpanKindServer.String(), "SPAN_KIND_"))), datadogPayload.Traces[0].Spans[0].Name)

	// ensure that span.type is based on otlp span.kind
	assert.Equal(t, "web", datadogPayload.Traces[0].Spans[0].Type)

	// ensure that span.meta and span.metrics pick up attributes, instrumentation ibrary and resource attribs
	assert.Equal(t, 12, len(datadogPayload.Traces[0].Spans[0].Meta))
	assert.Equal(t, 1, len(datadogPayload.Traces[0].Spans[0].Metrics))

	// ensure that span error is based on otlp span status
	assert.Equal(t, int32(0), datadogPayload.Traces[0].Spans[0].Error)

	// ensure that span meta also inccludes correctly sets resource attributes
	assert.Equal(t, "kube-system", datadogPayload.Traces[0].Spans[0].Meta["namespace"])

	// ensure that span service name gives resource service.name priority
	assert.Equal(t, "my_peer_service_name", datadogPayload.Traces[0].Spans[0].Service)

	// ensure a duration and start time are calculated
	assert.NotNil(t, datadogPayload.Traces[0].Spans[0].Start)
	assert.NotNil(t, datadogPayload.Traces[0].Spans[0].Duration)

	pdataMockEndTime := pdata.NewTimestampFromTime(mockEndTime)
	pdataMockStartTime := pdata.NewTimestampFromTime(mockEndTime.Add(-90 * time.Second))
	mockEventsString := fmt.Sprintf("[{\"attributes\":{},\"name\":\"start\",\"time\":%d},{\"attributes\":{\"flag\":false},\"name\":\"end\",\"time\":%d}]", pdataMockStartTime, pdataMockEndTime)

	// ensure that events tag is set if span events exist and contains structured json fields
	assert.Equal(t, mockEventsString, datadogPayload.Traces[0].Spans[0].Meta["events"])
}

// ensure that the datadog span uses the truncated tags if length exceeds max
func TestTracesTranslationTruncatetag(t *testing.T) {
	hostname := "testhostname"
	denylister := newDenylister([]string{})

	// generate mock trace, span and parent span ids
	mockTraceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	mockSpanID := [8]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}
	mockParentSpanID := [8]byte{0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8}
	mockEndTime := time.Now().Round(time.Second)

	// create mock resource span data
	// set shouldError and resourceServiceandEnv to false to test defaut behavior
	rs := NewResourceSpansData(mockTraceID, mockSpanID, mockParentSpanID, pdata.StatusCodeUnset, false, mockEndTime)

	span := rs.InstrumentationLibrarySpans().At(0).Spans().At(0)

	span.Attributes().InsertString(conventions.AttributeExceptionStacktrace, RandStringBytes(5500))

	// translate mocks to datadog traces
	datadogPayload := resourceSpansToDatadogSpans(rs, hostname, &config.Config{}, denylister, map[string]string{})
	// ensure we return the correct type
	assert.IsType(t, pb.TracePayload{}, datadogPayload)

	// ensure hostname arg is respected
	assert.Equal(t, hostname, datadogPayload.HostName)
	assert.Equal(t, 1, len(datadogPayload.Traces))

	// ensure trace id gets translated to uint64 correctly
	assert.Equal(t, decodeAPMTraceID(mockTraceID), datadogPayload.Traces[0].TraceID)

	// ensure the correct number of spans are expected
	assert.Equal(t, 1, len(datadogPayload.Traces[0].Spans))

	// ensure span's trace id matches payload trace id
	assert.Equal(t, datadogPayload.Traces[0].TraceID, datadogPayload.Traces[0].Spans[0].TraceID)

	// ensure span's spanId and parentSpanId are set correctly
	assert.Equal(t, decodeAPMSpanID(mockSpanID), datadogPayload.Traces[0].Spans[0].SpanID)
	assert.Equal(t, decodeAPMSpanID(mockParentSpanID), datadogPayload.Traces[0].Spans[0].ParentID)

	// ensure that span.resource defaults to otlp span.name
	assert.Equal(t, "End-To-End Here", datadogPayload.Traces[0].Spans[0].Resource)

	// ensure that span.name defaults to string representing instrumentation library if present
	assert.Equal(t, strings.ToLower(fmt.Sprintf("%s.%s", datadogPayload.Traces[0].Spans[0].Meta[conventions.InstrumentationLibraryName], strings.TrimPrefix(pdata.SpanKindServer.String(), "SPAN_KIND_"))), datadogPayload.Traces[0].Spans[0].Name)

	// ensure that span.type is based on otlp span.kind
	assert.Equal(t, "web", datadogPayload.Traces[0].Spans[0].Type)

	// ensure that span.meta and span.metrics pick up attributes, instrumentation ibrary and resource attribs
	assert.Equal(t, 12, len(datadogPayload.Traces[0].Spans[0].Meta))
	assert.Equal(t, 1, len(datadogPayload.Traces[0].Spans[0].Metrics))

	// ensure that span error is based on otlp span status
	assert.Equal(t, int32(0), datadogPayload.Traces[0].Spans[0].Error)

	// ensure that span meta also inccludes correctly sets resource attributes
	assert.Equal(t, "kube-system", datadogPayload.Traces[0].Spans[0].Meta["namespace"])

	// ensure that span service name gives resource service.name priority
	assert.Equal(t, 5000, len(datadogPayload.Traces[0].Spans[0].Meta[conventions.AttributeExceptionStacktrace]))

	// ensure a duration and start time are calculated
	assert.NotNil(t, datadogPayload.Traces[0].Spans[0].Start)
	assert.NotNil(t, datadogPayload.Traces[0].Spans[0].Duration)

	pdataMockEndTime := pdata.NewTimestampFromTime(mockEndTime)
	pdataMockStartTime := pdata.NewTimestampFromTime(mockEndTime.Add(-90 * time.Second))
	mockEventsString := fmt.Sprintf("[{\"attributes\":{},\"name\":\"start\",\"time\":%d},{\"attributes\":{\"flag\":false},\"name\":\"end\",\"time\":%d}]", pdataMockStartTime, pdataMockEndTime)

	// ensure that events tag is set if span events exist and contains structured json fields
	assert.Equal(t, mockEventsString, datadogPayload.Traces[0].Spans[0].Meta["events"])
}

// ensure that datadog span resource naming uses http method+route when available
func TestSpanResourceTranslation(t *testing.T) {
	span := pdata.NewSpan()
	span.SetKind(pdata.SpanKindServer)
	span.SetName("Default Name")

	ddHTTPTags := map[string]string{
		"http.method": "GET",
		"http.route":  "/api",
	}

	ddNotHTTPTags := map[string]string{
		"other": "GET",
	}

	resourceNameHTTP := getDatadogResourceName(span, ddHTTPTags)

	resourceNameDefault := getDatadogResourceName(span, ddNotHTTPTags)

	assert.Equal(t, "GET /api", resourceNameHTTP)
	assert.Equal(t, "Default Name", resourceNameDefault)
}

// ensure that datadog span resource naming uses http method+ grpc path when available
func TestSpanResourceTranslationGRPC(t *testing.T) {
	span := pdata.NewSpan()
	span.SetKind(pdata.SpanKindServer)
	span.SetName("Default Name")

	ddHTTPTags := map[string]string{
		"http.method": "POST",
		"grpc.path":   "/api",
	}

	ddNotHTTPTags := map[string]string{
		"other": "GET",
	}

	resourceNameHTTP := getDatadogResourceName(span, ddHTTPTags)

	resourceNameDefault := getDatadogResourceName(span, ddNotHTTPTags)

	assert.Equal(t, "POST /api", resourceNameHTTP)
	assert.Equal(t, "Default Name", resourceNameDefault)
}

// ensure that datadog span resource naming uses messaging operation+destination when available
func TestSpanResourceTranslationMessaging(t *testing.T) {
	span := pdata.NewSpan()
	span.SetKind(pdata.SpanKindServer)
	span.SetName("Default Name")

	ddHTTPTags := map[string]string{
		"messaging.operation":   "receive",
		"messaging.destination": "example.topic",
	}

	ddNotHTTPTags := map[string]string{
		"other": "GET",
	}

	resourceNameHTTP := getDatadogResourceName(span, ddHTTPTags)

	resourceNameDefault := getDatadogResourceName(span, ddNotHTTPTags)

	assert.Equal(t, "receive example.topic", resourceNameHTTP)
	assert.Equal(t, "Default Name", resourceNameDefault)
}

// ensure that datadog span resource naming uses messaging operation even when destination is not available
func TestSpanResourceTranslationMessagingFallback(t *testing.T) {
	span := pdata.NewSpan()
	span.SetKind(pdata.SpanKindServer)
	span.SetName("Default Name")

	ddHTTPTags := map[string]string{
		"messaging.operation": "receive",
	}

	ddNotHTTPTags := map[string]string{
		"other": "GET",
	}

	resourceNameHTTP := getDatadogResourceName(span, ddHTTPTags)

	resourceNameDefault := getDatadogResourceName(span, ddNotHTTPTags)

	assert.Equal(t, "receive", resourceNameHTTP)
	assert.Equal(t, "Default Name", resourceNameDefault)
}

// ensure that datadog span resource naming uses rpc method + rpc service when available
func TestSpanResourceTranslationRpc(t *testing.T) {
	span := pdata.NewSpan()
	span.SetKind(pdata.SpanKindServer)
	span.SetName("Default Name")

	ddHTTPTags := map[string]string{
		"rpc.method":  "example_method",
		"rpc.service": "example_service",
	}

	ddNotHTTPTags := map[string]string{
		"other": "GET",
	}

	resourceNameHTTP := getDatadogResourceName(span, ddHTTPTags)

	resourceNameDefault := getDatadogResourceName(span, ddNotHTTPTags)

	assert.Equal(t, "example_method example_service", resourceNameHTTP)
	assert.Equal(t, "Default Name", resourceNameDefault)
}

// ensure that datadog span resource naming uses rpc method even when rpc service is not available
func TestSpanResourceTranslationRpcFallback(t *testing.T) {
	span := pdata.NewSpan()
	span.SetKind(pdata.SpanKindServer)
	span.SetName("Default Name")

	ddHTTPTags := map[string]string{
		"rpc.method": "example_method",
	}

	ddNotHTTPTags := map[string]string{
		"other": "GET",
	}

	resourceNameHTTP := getDatadogResourceName(span, ddHTTPTags)

	resourceNameDefault := getDatadogResourceName(span, ddNotHTTPTags)

	assert.Equal(t, "example_method", resourceNameHTTP)
	assert.Equal(t, "Default Name", resourceNameDefault)
}

// ensure that the datadog span name uses IL name +kind when available and falls back to opetelemetry + kind
func TestSpanNameTranslation(t *testing.T) {
	span := pdata.NewSpan()
	span.SetName("Default Name")
	span.SetKind(pdata.SpanKindServer)

	ddIlTags := map[string]string{
		fmt.Sprintf(conventions.InstrumentationLibraryName): "il_name",
	}

	ddNoIlTags := map[string]string{
		"other": "other_value",
	}

	ddIlTagsOld := map[string]string{
		"otel.instrumentation_library.name": "old_value",
	}

	ddIlTagsCur := map[string]string{
		"otel.library.name": "current_value",
	}

	ddIlTagsUnusual := map[string]string{
		"otel.library.name": "@unusual/\\::value",
	}

	ddIlTagsHyphen := map[string]string{
		"otel.library.name": "hyphenated-value",
	}

	spanNameIl := getDatadogSpanName(span, ddIlTags)
	spanNameDefault := getDatadogSpanName(span, ddNoIlTags)
	spanNameOld := getDatadogSpanName(span, ddIlTagsOld)
	spanNameCur := getDatadogSpanName(span, ddIlTagsCur)
	spanNameUnusual := getDatadogSpanName(span, ddIlTagsUnusual)
	spanNameHyphen := getDatadogSpanName(span, ddIlTagsHyphen)

	assert.Equal(t, strings.ToLower(fmt.Sprintf("%s.%s", "il_name", strings.TrimPrefix(pdata.SpanKindServer.String(), "SPAN_KIND_"))), spanNameIl)
	assert.Equal(t, strings.ToLower(fmt.Sprintf("%s.%s", "opentelemetry", strings.TrimPrefix(pdata.SpanKindServer.String(), "SPAN_KIND_"))), spanNameDefault)
	assert.Equal(t, strings.ToLower(fmt.Sprintf("%s.%s", "old_value", strings.TrimPrefix(pdata.SpanKindServer.String(), "SPAN_KIND_"))), spanNameOld)
	assert.Equal(t, strings.ToLower(fmt.Sprintf("%s.%s", "current_value", strings.TrimPrefix(pdata.SpanKindServer.String(), "SPAN_KIND_"))), spanNameCur)
	assert.Equal(t, strings.ToLower(fmt.Sprintf("%s.%s", "unusual_value", strings.TrimPrefix(pdata.SpanKindServer.String(), "SPAN_KIND_"))), spanNameUnusual)
	assert.Equal(t, strings.ToLower(fmt.Sprintf("%s.%s", "hyphenated_value", strings.TrimPrefix(pdata.SpanKindServer.String(), "SPAN_KIND_"))), spanNameHyphen)
}

// ensure that the datadog span name uses IL name +kind when available and falls back to opetelemetry + kind
func TestSpanNameNormalization(t *testing.T) {
	emptyName := ""
	dashName := "k-e-b-a-b"
	camelCaseName := "camelCase"
	periodName := "first.second"
	removeSpacesName := "removes Spaces"
	numericsName := "num3r1cs_OK"
	underscoresName := "permits_underscores"
	tabName := "\t"
	junkName := "\tgetsRidOf\x1c\x1c\x18Junk"
	onlyJunkName := "\x02\x1c\x18\x08_only_junk_"
	onlyBadCharsName := "\x02\x1c\x18\x08"

	assert.Equal(t, utils.NormalizeSpanName(emptyName, false), "")
	assert.Equal(t, utils.NormalizeSpanName(dashName, false), "k_e_b_a_b")
	assert.Equal(t, utils.NormalizeSpanName(camelCaseName, false), "camelcase")
	assert.Equal(t, utils.NormalizeSpanName(periodName, false), "first.second")
	assert.Equal(t, utils.NormalizeSpanName(removeSpacesName, false), "removes_spaces")
	assert.Equal(t, utils.NormalizeSpanName(numericsName, false), "num3r1cs_ok")
	assert.Equal(t, utils.NormalizeSpanName(underscoresName, false), "permits_underscores")
	assert.Equal(t, utils.NormalizeSpanName(tabName, false), "")
	assert.Equal(t, utils.NormalizeSpanName(junkName, false), "getsridof_junk")
	assert.Equal(t, utils.NormalizeSpanName(onlyJunkName, false), "only_junk")
	assert.Equal(t, utils.NormalizeServiceName(dashName), "k-e-b-a-b")
	assert.Equal(t, utils.NormalizeServiceName(onlyBadCharsName), utils.DefaultServiceName)
	assert.Equal(t, utils.NormalizeServiceName(emptyName), utils.DefaultServiceName)
}

// ensure that the datadog span type gets mapped from span kind
func TestSpanTypeTranslation(t *testing.T) {
	spanTypeClient := inferDatadogType(pdata.SpanKindClient, map[string]string{})
	spanTypeServer := inferDatadogType(pdata.SpanKindServer, map[string]string{})
	spanTypeCustom := inferDatadogType(pdata.SpanKindUnspecified, map[string]string{})

	ddTagsDb := map[string]string{
		"db.system": "postgresql",
	}

	ddTagsCache := map[string]string{
		"db.system": "redis",
	}

	ddTagsCacheAlt := map[string]string{
		"db.system": "memcached",
	}

	spanTypeDb := inferDatadogType(pdata.SpanKindClient, ddTagsDb)
	spanTypeCache := inferDatadogType(pdata.SpanKindClient, ddTagsCache)
	spanTypeCacheAlt := inferDatadogType(pdata.SpanKindClient, ddTagsCacheAlt)

	assert.Equal(t, "http", spanTypeClient)
	assert.Equal(t, "web", spanTypeServer)
	assert.Equal(t, "custom", spanTypeCustom)
	assert.Equal(t, "db", spanTypeDb)
	assert.Equal(t, "cache", spanTypeCache)
	assert.Equal(t, "cache", spanTypeCacheAlt)
}

// ensure that the IL Tags extraction handles nil case
func TestILTagsExctraction(t *testing.T) {
	il := pdata.NewInstrumentationLibrary()

	tags := map[string]string{}

	extractInstrumentationLibraryTags(il, tags)

	assert.Equal(t, "", tags[conventions.InstrumentationLibraryName])

}

func TestHttpResourceTag(t *testing.T) {
	span := pdata.NewSpan()
	span.SetName("Default Name")
	span.SetKind(pdata.SpanKindServer)

	ddTags := map[string]string{
		"http.method": "POST",
	}

	resourceName := getDatadogResourceName(pdata.Span{}, ddTags)

	assert.Equal(t, "POST", resourceName)
}

// ensure that payloads get aggregated by env to reduce number of flushes
func TestTracePayloadAggr(t *testing.T) {

	// ensure that tracepayloads are aggregated by matchig hosts if envs match
	env := "testenv"
	payloadOne := pb.TracePayload{
		HostName:     "hostnameTestOne",
		Env:          env,
		Traces:       []*pb.APITrace{},
		Transactions: []*pb.Span{},
	}

	payloadTwo := pb.TracePayload{
		HostName:     "hostnameTestOne",
		Env:          env,
		Traces:       []*pb.APITrace{},
		Transactions: []*pb.Span{},
	}

	var originalPayload []*pb.TracePayload
	originalPayload = append(originalPayload, &payloadOne)
	originalPayload = append(originalPayload, &payloadTwo)

	updatedPayloads := aggregateTracePayloadsByEnv(originalPayload)

	assert.Equal(t, 2, len(originalPayload))
	assert.Equal(t, 1, len(updatedPayloads))

	// ensure that trace playloads are not aggregated by matching hosts if different envs
	envTwo := "testenvtwo"

	payloadThree := pb.TracePayload{
		HostName:     "hostnameTestOne",
		Env:          env,
		Traces:       []*pb.APITrace{},
		Transactions: []*pb.Span{},
	}

	payloadFour := pb.TracePayload{
		HostName:     "hostnameTestOne",
		Env:          envTwo,
		Traces:       []*pb.APITrace{},
		Transactions: []*pb.Span{},
	}

	var originalPayloadDifferentEnv []*pb.TracePayload
	originalPayloadDifferentEnv = append(originalPayloadDifferentEnv, &payloadThree)
	originalPayloadDifferentEnv = append(originalPayloadDifferentEnv, &payloadFour)

	updatedPayloadsDifferentEnv := aggregateTracePayloadsByEnv(originalPayloadDifferentEnv)

	assert.Equal(t, 2, len(originalPayloadDifferentEnv))
	assert.Equal(t, 2, len(updatedPayloadsDifferentEnv))
}

// ensure that stats payloads get tagged with version tag
func TestStatsAggregations(t *testing.T) {
	hostname := "testhostname"
	denylister := newDenylister([]string{})

	// generate mock trace, span and parent span ids
	mockTraceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	mockSpanID := [8]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}
	mockParentSpanID := [8]byte{0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8}

	mockEndTime := time.Now().Round(time.Second)

	// create mock resource span data
	// toggle on errors and custom service naming to test edge case code paths
	rs := NewResourceSpansData(mockTraceID, mockSpanID, mockParentSpanID, pdata.StatusCodeError, true, mockEndTime)

	// translate mocks to datadog traces
	cfg := config.Config{}

	datadogPayload := resourceSpansToDatadogSpans(rs, hostname, &cfg, denylister, map[string]string{})

	statsOutput := computeAPMStats(&datadogPayload, time.Now().UTC().UnixNano())

	var statsVersionTag stats.Tag
	var httpStatusCodeTag stats.Tag

	// extract the first stats.TagSet containing a stats.Tag of "version"
	for _, countVal := range statsOutput.Stats[0].Counts {
		for _, tagVal := range countVal.TagSet {
			if tagVal.Name == versionAggregationTag {
				statsVersionTag = tagVal
			}
			if tagVal.Name == httpStatusCodeAggregationTag {
				httpStatusCodeTag = tagVal
			}
		}
	}

	assert.Equal(t, "test-version", statsVersionTag.Value)
	assert.Equal(t, "501", httpStatusCodeTag.Value)
}

// ensure that stats payloads get adjusted for approriate sampling weight
func TestSamplingWeightedStatsAggregations(t *testing.T) {
	hostname := "testhostname"
	denylister := newDenylister([]string{})

	// generate mock trace, span and parent span ids
	mockTraceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	mockSpanID := [8]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}
	mockParentSpanID := [8]byte{0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8}

	mockEndTime := time.Now().Round(time.Second)

	// create mock resource span data
	// toggle on errors and custom service naming to test edge case code paths
	rs := NewResourceSpansData(mockTraceID, mockSpanID, mockParentSpanID, pdata.StatusCodeError, true, mockEndTime)

	rs.InstrumentationLibrarySpans().EnsureCapacity(1)
	ilss := rs.InstrumentationLibrarySpans().AppendEmpty()
	instrumentationLibrary := ilss.InstrumentationLibrary()
	instrumentationLibrary.SetName("flash")
	instrumentationLibrary.SetVersion("v1")
	span := ilss.Spans().AppendEmpty()
	span.Attributes().InsertString("_sample_rate", "0.2")
	span.SetKind(pdata.SpanKindServer)

	// translate mocks to datadog traces
	cfg := config.Config{}

	datadogPayload := resourceSpansToDatadogSpans(rs, hostname, &cfg, denylister, map[string]string{})

	statsOutput := computeAPMStats(&datadogPayload, time.Now().UTC().UnixNano())

	var WeightValue stats.Count

	// extract the first stats.Counts containing a Measure of "hits"
	// for a span name flash.server
	for _, countVal := range statsOutput.Stats[0].Counts {
		if countVal.Measure == "hits" && countVal.Name == "flash.server" {
			WeightValue = countVal
		}
	}

	// should equal 1.0 / 0.2 => 5
	assert.Equal(t, float64(5), WeightValue.Value)
}

// ensure that sanitization  of trace payloads occurs
func TestSanitization(t *testing.T) {
	denylister := newDenylister([]string{})
	buildInfo := component.BuildInfo{
		Version: "1.0",
	}

	traces := pdata.NewTraces()
	traces.ResourceSpans().EnsureCapacity(1)
	rs := traces.ResourceSpans().AppendEmpty()
	resource := rs.Resource()
	resource.Attributes().InsertString("deployment.environment", "UpperCase")
	rs.InstrumentationLibrarySpans().EnsureCapacity(1)
	ilss := rs.InstrumentationLibrarySpans().AppendEmpty()
	instrumentationLibrary := ilss.InstrumentationLibrary()
	instrumentationLibrary.SetName("flash")
	instrumentationLibrary.SetVersion("v1")
	ilss.Spans().EnsureCapacity(1)

	outputTraces, _ := convertToDatadogTd(traces, "test-host", &config.Config{}, denylister, buildInfo)

	aggregatedTraces := aggregateTracePayloadsByEnv(outputTraces)

	obfuscator := obfuscate.NewObfuscator(obfuscatorConfig)
	obfuscatePayload(obfuscator, aggregatedTraces)
	assert.Equal(t, 1, len(aggregatedTraces))

	assert.Equal(t, "uppercase", aggregatedTraces[0].Env)
}

func TestNormalizeTag(t *testing.T) {
	for _, tt := range []struct{ in, out string }{
		{in: "#test_starting_hash", out: "test_starting_hash"},
		{in: "TestCAPSandSuch", out: "testcapsandsuch"},
		{in: "Test Conversion Of Weird !@#$%^&**() Characters", out: "test_conversion_of_weird_characters"},
		{in: "$#weird_starting", out: "weird_starting"},
		{in: "allowed:c0l0ns", out: "allowed:c0l0ns"},
		{in: "1love", out: "love"},
		{in: "ünicöde", out: "ünicöde"},
		{in: "ünicöde:metäl", out: "ünicöde:metäl"},
		{in: "Data🐨dog🐶 繋がっ⛰てて", out: "data_dog_繋がっ_てて"},
		{in: " spaces   ", out: "spaces"},
		{in: " #hashtag!@#spaces #__<>#  ", out: "hashtag_spaces"},
		{in: ":testing", out: ":testing"},
		{in: "_foo", out: "foo"},
		{in: ":::test", out: ":::test"},
		{in: "contiguous_____underscores", out: "contiguous_underscores"},
		{in: "foo_", out: "foo"},
		{in: "\u017Fodd_\u017Fcase\u017F", out: "\u017Fodd_\u017Fcase\u017F"}, // edge-case
		{in: "", out: ""},
		{in: " ", out: ""},
		{in: "ok", out: "ok"},
		{in: "™Ö™Ö™™Ö™", out: "ö_ö_ö"},
		{in: "AlsO:ök", out: "also:ök"},
		{in: ":still_ok", out: ":still_ok"},
		{in: "___trim", out: "trim"},
		{in: "12.:trim@", out: ":trim"},
		{in: "12.:trim@@", out: ":trim"},
		{in: "fun:ky__tag/1", out: "fun:ky_tag/1"},
		{in: "fun:ky@tag/2", out: "fun:ky_tag/2"},
		{in: "fun:ky@@@tag/3", out: "fun:ky_tag/3"},
		{in: "tag:1/2.3", out: "tag:1/2.3"},
		{in: "---fun:k####y_ta@#g/1_@@#", out: "fun:k_y_ta_g/1"},
		{in: "AlsO:œ#@ö))œk", out: "also:œ_ö_œk"},
		{in: "test\x99\x8faaa", out: "test_aaa"},
		{in: "test\x99\x8f", out: "test"},
		{in: strings.Repeat("a", 888), out: strings.Repeat("a", 200)},
		{
			in: func() string {
				b := bytes.NewBufferString("a")
				for i := 0; i < 799; i++ {
					_, err := b.WriteRune('🐶')
					assert.NoError(t, err)
				}
				_, err := b.WriteRune('b')
				assert.NoError(t, err)
				return b.String()
			}(),
			out: "a", // 'b' should have been truncated
		},
		{"a" + string(unicode.ReplacementChar), "a"},
		{"a" + string(unicode.ReplacementChar) + string(unicode.ReplacementChar), "a"},
		{"a" + string(unicode.ReplacementChar) + string(unicode.ReplacementChar) + "b", "a_b"},
	} {
		t.Run("", func(t *testing.T) {
			assert.Equal(t, tt.out, utils.NormalizeTag(tt.in), tt.in)
		})
	}
}

// ensure that sanitization  of trace payloads occurs
func TestSpanNameMapping(t *testing.T) {
	mockTraceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	mockSpanID := [8]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}
	mockParentSpanID := [8]byte{0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8}
	endTime := time.Now().Round(time.Second)
	pdataEndTime := pdata.NewTimestampFromTime(endTime)
	startTime := endTime.Add(-90 * time.Second)
	pdataStartTime := pdata.NewTimestampFromTime(startTime)

	denylister := newDenylister([]string{})
	buildInfo := component.BuildInfo{
		Version: "1.0",
	}

	traces := pdata.NewTraces()
	traces.ResourceSpans().EnsureCapacity(1)
	rs := traces.ResourceSpans().AppendEmpty()
	resource := rs.Resource()
	resource.Attributes().InsertString("deployment.environment", "UpperCase")
	rs.InstrumentationLibrarySpans().EnsureCapacity(1)
	ilss := rs.InstrumentationLibrarySpans().AppendEmpty()
	instrumentationLibrary := ilss.InstrumentationLibrary()
	instrumentationLibrary.SetName("flash")
	instrumentationLibrary.SetVersion("v1")
	span := ilss.Spans().AppendEmpty()

	traceID := pdata.NewTraceID(mockTraceID)
	spanID := pdata.NewSpanID(mockSpanID)
	parentSpanID := pdata.NewSpanID(mockParentSpanID)
	span.SetTraceID(traceID)
	span.SetSpanID(spanID)
	span.SetParentSpanID(parentSpanID)
	span.SetName("End-To-End Here")
	span.SetKind(pdata.SpanKindServer)
	span.SetStartTimestamp(pdataStartTime)
	span.SetEndTimestamp(pdataEndTime)

	config := config.Config{Traces: config.TracesConfig{SpanNameRemappings: map[string]string{"flash.server": "bang.client"}}}

	outputTraces, _ := convertToDatadogTd(traces, "test-host", &config, denylister, buildInfo)
	aggregatedTraces := aggregateTracePayloadsByEnv(outputTraces)

	obfuscator := obfuscate.NewObfuscator(obfuscatorConfig)
	obfuscatePayload(obfuscator, aggregatedTraces)
	assert.Equal(t, 1, len(aggregatedTraces))

	assert.Equal(t, "bang.client", aggregatedTraces[0].Traces[0].Spans[0].Name)
}

// ensure that sanitization  of trace payloads occurs
func TestSpanEnvClobbering(t *testing.T) {
	mockTraceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	mockSpanID := [8]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}
	mockParentSpanID := [8]byte{0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8}
	endTime := time.Now().Round(time.Second)
	pdataEndTime := pdata.NewTimestampFromTime(endTime)
	startTime := endTime.Add(-90 * time.Second)
	pdataStartTime := pdata.NewTimestampFromTime(startTime)

	denylister := newDenylister([]string{})
	buildInfo := component.BuildInfo{
		Version: "1.0",
	}

	traces := pdata.NewTraces()
	traces.ResourceSpans().EnsureCapacity(1)
	rs := traces.ResourceSpans().AppendEmpty()
	resource := rs.Resource()
	resource.Attributes().InsertString(conventions.AttributeDeploymentEnvironment, "correctenv")
	resource.Attributes().InsertString("env", "incorrectenv")

	rs.InstrumentationLibrarySpans().EnsureCapacity(1)
	ilss := rs.InstrumentationLibrarySpans().AppendEmpty()
	instrumentationLibrary := ilss.InstrumentationLibrary()
	instrumentationLibrary.SetName("flash")
	instrumentationLibrary.SetVersion("v1")
	span := ilss.Spans().AppendEmpty()

	traceID := pdata.NewTraceID(mockTraceID)
	spanID := pdata.NewSpanID(mockSpanID)
	parentSpanID := pdata.NewSpanID(mockParentSpanID)
	span.SetTraceID(traceID)
	span.SetSpanID(spanID)
	span.SetParentSpanID(parentSpanID)
	span.SetName("End-To-End Here")
	span.SetKind(pdata.SpanKindServer)
	span.SetStartTimestamp(pdataStartTime)
	span.SetEndTimestamp(pdataEndTime)

	outputTraces, _ := convertToDatadogTd(traces, "test-host", &config.Config{}, denylister, buildInfo)

	// Ensure the deployment.environment value is copied to both deployment.environment and env
	assert.Equal(t, "correctenv", outputTraces[0].Traces[0].Spans[0].Meta["env"])
	assert.Equal(t, "correctenv", outputTraces[0].Traces[0].Spans[0].Meta["deployment.environment"])
}

// ensure that global rate limit tag gets set as a metric
func TestSpanRateLimitTag(t *testing.T) {
	mockTraceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	mockSpanID := [8]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}
	mockParentSpanID := [8]byte{0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8}
	endTime := time.Now().Round(time.Second)
	pdataEndTime := pdata.NewTimestampFromTime(endTime)
	startTime := endTime.Add(-90 * time.Second)
	pdataStartTime := pdata.NewTimestampFromTime(startTime)

	denylister := newDenylister([]string{})
	buildInfo := component.BuildInfo{
		Version: "1.0",
	}

	traces := pdata.NewTraces()
	traces.ResourceSpans().EnsureCapacity(1)
	rs := traces.ResourceSpans().AppendEmpty()
	resource := rs.Resource()
	resource.Attributes().InsertString(conventions.AttributeDeploymentEnvironment, "correctenv")

	rs.InstrumentationLibrarySpans().EnsureCapacity(1)
	ilss := rs.InstrumentationLibrarySpans().AppendEmpty()
	instrumentationLibrary := ilss.InstrumentationLibrary()
	instrumentationLibrary.SetName("flash")
	instrumentationLibrary.SetVersion("v1")
	span := ilss.Spans().AppendEmpty()

	attribs := map[string]pdata.AttributeValue{
		"_sample_rate": pdata.NewAttributeValueString("0.5"),
	}

	pdata.NewAttributeMapFromMap(attribs).CopyTo(span.Attributes())

	traceID := pdata.NewTraceID(mockTraceID)
	spanID := pdata.NewSpanID(mockSpanID)
	parentSpanID := pdata.NewSpanID(mockParentSpanID)
	span.SetTraceID(traceID)
	span.SetSpanID(spanID)
	span.SetParentSpanID(parentSpanID)
	span.SetName("End-To-End Here")
	span.SetKind(pdata.SpanKindServer)
	span.SetStartTimestamp(pdataStartTime)
	span.SetEndTimestamp(pdataEndTime)

	outputTraces, _ := convertToDatadogTd(traces, "test-host", &config.Config{}, denylister, buildInfo)

	assert.Equal(t, 0.5, outputTraces[0].Traces[0].Spans[0].Metrics["_sample_rate"])
}

func TestTracesSpanNamingOption(t *testing.T) {
	hostname := "testhostname"
	denylister := newDenylister([]string{})

	// generate mock trace, span and parent span ids
	mockTraceID := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	mockSpanID := [8]byte{0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8}
	mockParentSpanID := [8]byte{0xEF, 0xEE, 0xED, 0xEC, 0xEB, 0xEA, 0xE9, 0xE8}

	mockEndTime := time.Now().Round(time.Second)

	// create mock resource span data
	// toggle on errors and custom service naming to test edge case code paths
	rs := NewResourceSpansData(mockTraceID, mockSpanID, mockParentSpanID, pdata.StatusCodeUnset, false, mockEndTime)

	// start with span name as resource name set to true
	cfgSpanNameAsResourceName := config.Config{
		Traces: config.TracesConfig{
			SpanNameAsResourceName: true,
		},
	}

	// translate mocks to datadog traces
	datadogPayloadSpanNameAsResourceName := resourceSpansToDatadogSpans(rs, hostname, &cfgSpanNameAsResourceName, denylister, map[string]string{})

	// ensure the resource name is replaced with the span name when the option is set
	assert.Equal(t, "End-To-End Here", datadogPayloadSpanNameAsResourceName.Traces[0].Spans[0].Name)
}
