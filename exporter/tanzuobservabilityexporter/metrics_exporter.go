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

package tanzuobservabilityexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/tanzuobservabilityexporter"

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/wavefronthq/wavefront-sdk-go/senders"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/model/pdata"
)

type metricsExporter struct {
	consumer *metricsConsumer
}

func createMetricsConsumer(hostName string, port int, settings component.TelemetrySettings, otelVersion string) (*metricsConsumer, error) {
	s, err := senders.NewProxySender(&senders.ProxyConfiguration{
		Host:                 hostName,
		MetricsPort:          port,
		FlushIntervalSeconds: 1,
		SDKMetricsTags:       map[string]string{"otel.metrics.collector_version": otelVersion},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create proxy sender: %v", err)
	}
	cumulative := newCumulativeHistogramDataPointConsumer(s)
	delta := newDeltaHistogramDataPointConsumer(s)
	return newMetricsConsumer(
		[]typedMetricConsumer{
			newGaugeConsumer(s, settings),
			newSumConsumer(s, settings),
			newHistogramConsumer(cumulative, delta, s, regularHistogram, settings),
			newHistogramConsumer(cumulative, delta, s, exponentialHistogram, settings),
			newSummaryConsumer(s, settings),
		},
		s,
		true), nil
}

type metricsConsumerCreator func(hostName string, port int, settings component.TelemetrySettings, otelVersion string) (
	*metricsConsumer, error)

func newMetricsExporter(settings component.ExporterCreateSettings, c config.Exporter, creator metricsConsumerCreator) (*metricsExporter, error) {
	cfg, ok := c.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid config: %#v", c)
	}
	endpoint, err := url.Parse(cfg.Metrics.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metrics.endpoint: %v", err)
	}
	metricsPort, err := strconv.Atoi(endpoint.Port())
	if err != nil {
		// The port is empty, otherwise url.Parse would have failed above
		return nil, fmt.Errorf("metrics.endpoint requires a port")
	}
	consumer, err := creator(endpoint.Hostname(), metricsPort, settings.TelemetrySettings, settings.BuildInfo.Version)
	if err != nil {
		return nil, err
	}
	return &metricsExporter{
		consumer: consumer,
	}, nil
}

func (e *metricsExporter) pushMetricsData(ctx context.Context, md pdata.Metrics) error {
	return e.consumer.Consume(ctx, md)
}

func (e *metricsExporter) shutdown(_ context.Context) error {
	e.consumer.Close()
	return nil
}
