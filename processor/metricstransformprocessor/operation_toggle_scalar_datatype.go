// Copyright 2020 OpenTelemetry Authors
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

package metricstransformprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/metricstransformprocessor"

import metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"

func (mtp *metricsTransformProcessor) ToggleScalarDataType(metric *metricspb.Metric) {
	for _, ts := range metric.Timeseries {
		for _, dp := range ts.Points {
			switch metric.MetricDescriptor.Type {
			case metricspb.MetricDescriptor_GAUGE_INT64, metricspb.MetricDescriptor_CUMULATIVE_INT64:
				dp.Value = &metricspb.Point_DoubleValue{DoubleValue: float64(dp.GetInt64Value())}
			case metricspb.MetricDescriptor_GAUGE_DOUBLE, metricspb.MetricDescriptor_CUMULATIVE_DOUBLE:
				dp.Value = &metricspb.Point_Int64Value{Int64Value: int64(dp.GetDoubleValue())}
			}
		}
	}

	switch metric.MetricDescriptor.Type {
	case metricspb.MetricDescriptor_GAUGE_INT64:
		metric.MetricDescriptor.Type = metricspb.MetricDescriptor_GAUGE_DOUBLE
	case metricspb.MetricDescriptor_CUMULATIVE_INT64:
		metric.MetricDescriptor.Type = metricspb.MetricDescriptor_CUMULATIVE_DOUBLE
	case metricspb.MetricDescriptor_GAUGE_DOUBLE:
		metric.MetricDescriptor.Type = metricspb.MetricDescriptor_GAUGE_INT64
	case metricspb.MetricDescriptor_CUMULATIVE_DOUBLE:
		metric.MetricDescriptor.Type = metricspb.MetricDescriptor_CUMULATIVE_INT64
	}
}
