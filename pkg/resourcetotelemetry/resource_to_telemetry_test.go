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
package resourcetotelemetry

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/testdata"
)

func TestConvertResourceToAttributes(t *testing.T) {
	md := testdata.GenerateMetricsOneMetric()
	assert.NotNil(t, md)

	// Before converting resource to labels
	assert.Equal(t, 1, md.ResourceMetrics().At(0).Resource().Attributes().Len())
	assert.Equal(t, 1, md.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0).Attributes().Len())

	cloneMd := convertToMetricsAttributes(md)

	// After converting resource to labels
	assert.Equal(t, 1, cloneMd.ResourceMetrics().At(0).Resource().Attributes().Len())
	assert.Equal(t, 2, cloneMd.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0).Attributes().Len())

	assert.Equal(t, 1, md.ResourceMetrics().At(0).Resource().Attributes().Len())
	assert.Equal(t, 1, md.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0).Attributes().Len())

}

func TestConvertResourceToAttributesAllDataTypesEmptyDataPoint(t *testing.T) {
	md := testdata.GenerateMetricsAllTypesEmptyDataPoint()
	assert.NotNil(t, md)

	// Before converting resource to labels
	assert.Equal(t, 1, md.ResourceMetrics().At(0).Resource().Attributes().Len())
	assert.Equal(t, 0, md.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 0, md.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(1).Gauge().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 0, md.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(2).Sum().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 0, md.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(3).Sum().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 0, md.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(4).Histogram().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 0, md.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(5).Summary().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 0, md.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(6).ExponentialHistogram().DataPoints().At(0).Attributes().Len())

	cloneMd := convertToMetricsAttributes(md)

	// After converting resource to labels
	assert.Equal(t, 1, cloneMd.ResourceMetrics().At(0).Resource().Attributes().Len())
	assert.Equal(t, 1, cloneMd.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 1, cloneMd.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(1).Gauge().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 1, cloneMd.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(2).Sum().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 1, cloneMd.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(3).Sum().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 1, cloneMd.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(4).Histogram().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 1, cloneMd.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(5).Summary().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 1, cloneMd.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(6).ExponentialHistogram().DataPoints().At(0).Attributes().Len())

	assert.Equal(t, 1, md.ResourceMetrics().At(0).Resource().Attributes().Len())
	assert.Equal(t, 0, md.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 0, md.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(1).Gauge().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 0, md.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(2).Sum().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 0, md.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(3).Sum().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 0, md.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(4).Histogram().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 0, md.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(5).Summary().DataPoints().At(0).Attributes().Len())
	assert.Equal(t, 0, md.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(6).ExponentialHistogram().DataPoints().At(0).Attributes().Len())

}
