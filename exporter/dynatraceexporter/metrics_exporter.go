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

package dynatraceexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/dynatraceexporter"

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/dynatrace-oss/dynatrace-metric-utils-go/metric/apiconstants"
	"github.com/dynatrace-oss/dynatrace-metric-utils-go/metric/dimensions"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/dynatraceexporter/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/dynatraceexporter/serialization"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/ttlmap"
)

const (
	cSweepIntervalSeconds = 300
	cMaxAgeSeconds        = 900
)

// NewExporter exports to a Dynatrace Metrics v2 API
func newMetricsExporter(params component.ExporterCreateSettings, cfg *config.Config) *exporter {
	confDefaultDims := []dimensions.Dimension{}
	for key, value := range cfg.DefaultDimensions {
		confDefaultDims = append(confDefaultDims, dimensions.NewDimension(key, value))
	}

	defaultDimensions := dimensions.MergeLists(
		dimensionsFromTags(cfg.Tags),
		dimensions.NewNormalizedDimensionList(confDefaultDims...),
	)

	staticDimensions := dimensions.NewNormalizedDimensionList(dimensions.NewDimension("dt.metrics.source", "opentelemetry"))

	prevPts := ttlmap.New(cSweepIntervalSeconds, cMaxAgeSeconds)
	prevPts.Start()

	return &exporter{
		settings:          params.TelemetrySettings,
		cfg:               cfg,
		defaultDimensions: defaultDimensions,
		staticDimensions:  staticDimensions,
		prevPts:           prevPts,
	}
}

// exporter forwards metrics to a Dynatrace agent
type exporter struct {
	settings   component.TelemetrySettings
	cfg        *config.Config
	client     *http.Client
	isDisabled bool

	defaultDimensions dimensions.NormalizedDimensionList
	staticDimensions  dimensions.NormalizedDimensionList

	prevPts *ttlmap.TTLMap
}

// for backwards-compatibility with deprecated `Tags` config option
func dimensionsFromTags(tags []string) dimensions.NormalizedDimensionList {
	dims := []dimensions.Dimension{}
	for _, tag := range tags {
		parts := strings.SplitN(tag, "=", 2)
		if len(parts) == 2 {
			dims = append(dims, dimensions.NewDimension(parts[0], parts[1]))
		}
	}
	return dimensions.NewNormalizedDimensionList(dims...)
}

func (e *exporter) PushMetricsData(ctx context.Context, md pdata.Metrics) error {
	if e.isDisabled {
		return nil
	}

	lines := e.serializeMetrics(md)
	e.settings.Logger.Sugar().Debugw("Serialization complete",
		"DataPoints", md.DataPointCount(),
		"lines", len(lines),
	)

	// If request is empty string, there are no serializable metrics in the batch.
	// This can happen if all metric names are invalid
	if len(lines) == 0 {
		return nil
	}

	err := e.send(ctx, lines)

	if err != nil {
		return err
	}

	return nil
}

func (e *exporter) serializeMetrics(md pdata.Metrics) []string {
	lines := make([]string, 0)

	resourceMetrics := md.ResourceMetrics()

	for i := 0; i < resourceMetrics.Len(); i++ {
		resourceMetric := resourceMetrics.At(i)
		libraryMetrics := resourceMetric.InstrumentationLibraryMetrics()
		for j := 0; j < libraryMetrics.Len(); j++ {
			libraryMetric := libraryMetrics.At(j)
			metrics := libraryMetric.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)

				metricLines, err := serialization.SerializeMetric(e.settings.Logger, e.cfg.Prefix, metric, e.defaultDimensions, e.staticDimensions, e.prevPts)

				if err != nil {
					e.settings.Logger.Sugar().Errorw(
						"failed to serialize",
						"datatype", metric.DataType().String(),
						"name", metric.Name(),
						zap.Error(err),
					)
				}

				if len(metricLines) > 0 {
					lines = append(lines, metricLines...)
				}
				e.settings.Logger.Sugar().Debugw("Serialized metric data",
					"metric-type", metric.DataType().String(),
					"metric-name", metric.Name(),
					"data-len", len(metricLines),
				)
			}
		}
	}

	return lines
}

var lastLog int64

// send sends a serialized metric batch to Dynatrace.
// An error indicates all lines were dropped regardless of the returned number.
func (e *exporter) send(ctx context.Context, lines []string) error {
	e.settings.Logger.Debug("Exporting", zap.Int("lines", len(lines)))

	if now := time.Now().Unix(); len(lines) > apiconstants.GetPayloadLinesLimit() && now-lastLog > 60 {
		e.settings.Logger.Sugar().Warnf("Batch too large. Sending in chunks of %[1]d metrics. If any chunk fails, previous chunks in the batch could be retried by the batch processor. Please set send_batch_max_size to %[1]d or less. Suppressing this log for 60 seconds.", apiconstants.GetPayloadLinesLimit())
		lastLog = time.Now().Unix()
	}

	for i := 0; i < len(lines); i += apiconstants.GetPayloadLinesLimit() {
		end := i + apiconstants.GetPayloadLinesLimit()

		if end > len(lines) {
			end = len(lines)
		}

		err := e.sendBatch(ctx, lines[i:end])
		if err != nil {
			return err
		}
	}

	return nil
}

// send sends a serialized metric batch to Dynatrace.
// An error indicates all lines were dropped regardless of the returned number.
func (e *exporter) sendBatch(ctx context.Context, lines []string) error {
	message := strings.Join(lines, "\n")
	e.settings.Logger.Sugar().Debugw("SendBatch",
		"lines", len(lines),
		"endpoint", e.cfg.Endpoint,
	)

	req, err := http.NewRequestWithContext(ctx, "POST", e.cfg.Endpoint, bytes.NewBufferString(message))

	if err != nil {
		return consumererror.NewPermanent(err)
	}

	e.settings.Logger.Debug("Sending request")
	resp, err := e.client.Do(req)

	if err != nil {
		e.settings.Logger.Error("failed to send request", zap.Error(err))
		return fmt.Errorf("sendBatch: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusRequestEntityTooLarge {
		// If a payload is too large, resending it will not help
		return consumererror.NewPermanent(fmt.Errorf("payload too large"))
	}

	if resp.StatusCode == http.StatusBadRequest {
		// At least some metrics were not accepted
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			// if the response cannot be read, do not retry the batch as it may have been successful
			e.settings.Logger.Error("Failed to read response from Dynatrace", zap.Error(err))
			return nil
		}

		responseBody := metricsResponse{}
		if err := json.Unmarshal(bodyBytes, &responseBody); err != nil {
			// if the response cannot be read, do not retry the batch as it may have been successful
			e.settings.Logger.Error("Failed to unmarshal response from Dynatrace", zap.Error(err), zap.ByteString("body", bodyBytes))
			return nil
		}

		e.settings.Logger.Sugar().Errorw("Response from Dynatrace",
			"accepted-lines", responseBody.Ok,
			"rejected-lines", responseBody.Invalid,
			"error-message", responseBody.Error.Message,
			"status", resp.Status,
		)

		for _, line := range responseBody.Error.InvalidLines {
			// Enabled debug logging to see which lines were dropped
			if line.Line >= 0 && line.Line < len(lines) {
				e.settings.Logger.Sugar().Debugf("rejected line %3d: [%s] %s", line.Line, line.Error, lines[line.Line])
			}
		}

		return nil
	}

	if resp.StatusCode == http.StatusUnauthorized {
		// token is missing or wrong format
		e.isDisabled = true
		return consumererror.NewPermanent(fmt.Errorf("API token missing or invalid"))
	}

	if resp.StatusCode == http.StatusForbidden {
		return consumererror.NewPermanent(fmt.Errorf("API token missing the required scope (metrics.ingest)"))
	}

	if resp.StatusCode == http.StatusNotFound {
		return consumererror.NewPermanent(fmt.Errorf("metrics ingest v2 module not found - ensure module is enabled and endpoint is correct"))
	}

	// No known errors
	return nil
}

// start starts the exporter
func (e *exporter) start(_ context.Context, host component.Host) (err error) {
	client, err := e.cfg.HTTPClientSettings.ToClient(host.GetExtensions(), e.settings)
	if err != nil {
		e.settings.Logger.Error("Failed to construct HTTP client", zap.Error(err))
		return fmt.Errorf("start: %w", err)
	}

	e.client = client

	return nil
}

// Response from Dynatrace is expected to be in JSON format
type metricsResponse struct {
	Ok      int                  `json:"linesOk"`
	Invalid int                  `json:"linesInvalid"`
	Error   metricsResponseError `json:"error"`
}

type metricsResponseError struct {
	Code         int                               `json:"code"`
	Message      string                            `json:"message"`
	InvalidLines []metricsResponseErrorInvalidLine `json:"invalidLines"`
}

type metricsResponseErrorInvalidLine struct {
	Line  int    `json:"line"`
	Error string `json:"error"`
}
