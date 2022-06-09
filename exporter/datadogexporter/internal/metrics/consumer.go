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

package metrics // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/internal/metrics"

import (
	"context"

	"github.com/DataDog/datadog-agent/pkg/quantile"
	"go.opentelemetry.io/collector/component"
	"gopkg.in/zorkian/go-datadog-api.v2"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/internal/model/translator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/internal/sketches"
)

var _ translator.Consumer = (*Consumer)(nil)
var _ translator.HostConsumer = (*Consumer)(nil)
var _ translator.TagsConsumer = (*Consumer)(nil)

// Consumer is the metrics Consumer.
type Consumer struct {
	ms        []datadog.Metric
	sl        sketches.SketchSeriesList
	seenHosts map[string]struct{}
	seenTags  map[string]struct{}
}

// NewConsumer creates a new zorkian consumer.
func NewConsumer() *Consumer {
	return &Consumer{
		seenHosts: make(map[string]struct{}),
		seenTags:  make(map[string]struct{}),
	}
}

// toDataType maps translator datatypes to zorkian's datatypes.
func (c *Consumer) toDataType(dt translator.MetricDataType) (out MetricDataType) {
	out = MetricDataType("unknown")

	switch dt {
	case translator.Count:
		out = Count
	case translator.Gauge:
		out = Gauge
	}

	return
}

// runningMetrics gets the running metrics for the exporter.
func (c *Consumer) runningMetrics(timestamp uint64, buildInfo component.BuildInfo) (series []datadog.Metric) {
	for host := range c.seenHosts {
		// Report the host as running
		runningMetric := DefaultMetrics("metrics", host, timestamp, buildInfo)
		series = append(series, runningMetric...)
	}

	for tag := range c.seenTags {
		runningMetrics := DefaultMetrics("metrics", "", timestamp, buildInfo)
		for i := range runningMetrics {
			runningMetrics[i].Tags = append(runningMetrics[i].Tags, tag)
		}
		series = append(series, runningMetrics...)
	}

	return
}

// All gets all metrics (consumed metrics and running metrics).
func (c *Consumer) All(timestamp uint64, buildInfo component.BuildInfo) ([]datadog.Metric, sketches.SketchSeriesList) {
	series := c.ms
	series = append(series, c.runningMetrics(timestamp, buildInfo)...)
	return series, c.sl
}

// ConsumeTimeSeries implements the translator.Consumer interface.
func (c *Consumer) ConsumeTimeSeries(
	_ context.Context,
	name string,
	typ translator.MetricDataType,
	timestamp uint64,
	value float64,
	tags []string,
	host string,
) {
	dt := c.toDataType(typ)
	met := NewMetric(name, dt, timestamp, value, tags)
	met.SetHost(host)
	c.ms = append(c.ms, met)
}

// ConsumeSketch implements the translator.Consumer interface.
func (c *Consumer) ConsumeSketch(
	_ context.Context,
	name string,
	timestamp uint64,
	sketch *quantile.Sketch,
	tags []string,
	host string,
) {
	c.sl = append(c.sl, sketches.SketchSeries{
		Name:     name,
		Tags:     tags,
		Host:     host,
		Interval: 1,
		Points: []sketches.SketchPoint{{
			Ts:     int64(timestamp / 1e9),
			Sketch: sketch,
		}},
	})
}

// ConsumeHost implements the translator.HostConsumer interface.
func (c *Consumer) ConsumeHost(host string) {
	c.seenHosts[host] = struct{}{}
}

// ConsumeTag implements the translator.TagsConsumer interface.
func (c *Consumer) ConsumeTag(tag string) {
	c.seenTags[tag] = struct{}{}
}
