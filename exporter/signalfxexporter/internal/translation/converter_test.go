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
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	sfxpb "github.com/signalfx/com_signalfx_metrics_protobuf/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/model/pdata"
	conventions "go.opentelemetry.io/collector/model/semconv/v1.5.0"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/signalfxexporter/internal/translation/dpfilters"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/maps"
)

const (
	unixSecs  = int64(1574092046)
	unixNSecs = int64(11 * time.Millisecond)
	tsMSecs   = unixSecs*1e3 + unixNSecs/1e6
)

// Not const to be able to take the address of them.
var (
	doubleVal = 1234.5678
	int64Val  = int64(123)
)

func Test_MetricDataToSignalFxV2(t *testing.T) {
	logger := zap.NewNop()

	labelMap := map[string]string{
		"k0": "v0",
		"k1": "v1",
	}

	longLabelMap := map[string]string{
		fmt.Sprintf("l%sng_key", strings.Repeat("o", 128)): "v0",
		"k0": "v0",
		"k1": fmt.Sprintf("l%sng_value", strings.Repeat("o", 256)),
		"k2": "v2",
	}

	ts := pdata.NewTimestampFromTime(time.Unix(unixSecs, unixNSecs))

	initDoublePt := func(doublePt pdata.NumberDataPoint) {
		doublePt.SetTimestamp(ts)
		doublePt.SetDoubleVal(doubleVal)
	}

	initDoublePtWithLabels := func(doublePtWithLabels pdata.NumberDataPoint) {
		initDoublePt(doublePtWithLabels)
		pdata.NewAttributeMapFromMap(stringMapToAttributeMap(labelMap)).CopyTo(doublePtWithLabels.Attributes())
	}

	initDoublePtWithLongLabels := func(doublePtWithLabels pdata.NumberDataPoint) {
		initDoublePt(doublePtWithLabels)
		pdata.NewAttributeMapFromMap(stringMapToAttributeMap(longLabelMap)).CopyTo(doublePtWithLabels.Attributes())
	}

	differentLabelMap := map[string]string{
		"k00": "v00",
		"k11": "v11",
	}
	initDoublePtWithDifferentLabels := func(doublePtWithDifferentLabels pdata.NumberDataPoint) {
		initDoublePt(doublePtWithDifferentLabels)
		pdata.NewAttributeMapFromMap(stringMapToAttributeMap(differentLabelMap)).CopyTo(doublePtWithDifferentLabels.Attributes())
	}

	initInt64Pt := func(int64Pt pdata.NumberDataPoint) {
		int64Pt.SetTimestamp(ts)
		int64Pt.SetIntVal(int64Val)
	}

	initInt64PtWithLabels := func(int64PtWithLabels pdata.NumberDataPoint) {
		initInt64Pt(int64PtWithLabels)
		pdata.NewAttributeMapFromMap(stringMapToAttributeMap(labelMap)).CopyTo(int64PtWithLabels.Attributes())
	}

	histBounds := []float64{1, 2, 4}
	histCounts := []uint64{4, 2, 3, 7}

	initHistDP := func(histDP pdata.HistogramDataPoint) {
		histDP.SetTimestamp(ts)
		histDP.SetCount(16)
		histDP.SetSum(100.0)
		histDP.SetExplicitBounds(histBounds)
		histDP.SetBucketCounts(histCounts)
		pdata.NewAttributeMapFromMap(stringMapToAttributeMap(labelMap)).CopyTo(histDP.Attributes())
	}
	histDP := pdata.NewHistogramDataPoint()
	initHistDP(histDP)

	initHistDPNoBuckets := func(histDP pdata.HistogramDataPoint) {
		histDP.SetCount(2)
		histDP.SetSum(10)
		histDP.SetTimestamp(ts)
		pdata.NewAttributeMapFromMap(stringMapToAttributeMap(labelMap)).CopyTo(histDP.Attributes())
	}
	histDPNoBuckets := pdata.NewHistogramDataPoint()
	initHistDPNoBuckets(histDPNoBuckets)

	tests := []struct {
		name              string
		metricsFn         func() pdata.Metrics
		excludeMetrics    []dpfilters.MetricFilter
		includeMetrics    []dpfilters.MetricFilter
		wantSfxDataPoints []*sfxpb.DataPoint
	}{
		{
			name: "nil_node_nil_resources_no_dims",
			metricsFn: func() pdata.Metrics {
				out := pdata.NewMetrics()
				ilm := out.ResourceMetrics().AppendEmpty().InstrumentationLibraryMetrics().AppendEmpty()

				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("gauge_double_with_dims")
					m.SetDataType(pdata.MetricDataTypeGauge)
					initDoublePt(m.Gauge().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("gauge_int_with_dims")
					m.SetDataType(pdata.MetricDataTypeGauge)
					initInt64Pt(m.Gauge().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("cumulative_double_with_dims")
					m.SetDataType(pdata.MetricDataTypeSum)
					m.Sum().SetIsMonotonic(true)
					m.Sum().SetAggregationTemporality(pdata.MetricAggregationTemporalityCumulative)
					initDoublePt(m.Sum().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("cumulative_int_with_dims")
					m.SetDataType(pdata.MetricDataTypeSum)
					m.Sum().SetIsMonotonic(true)
					m.Sum().SetAggregationTemporality(pdata.MetricAggregationTemporalityCumulative)
					initInt64Pt(m.Sum().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("delta_double_with_dims")
					m.SetDataType(pdata.MetricDataTypeSum)
					m.Sum().SetIsMonotonic(true)
					m.Sum().SetAggregationTemporality(pdata.MetricAggregationTemporalityDelta)
					initDoublePt(m.Sum().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("delta_int_with_dims")
					m.SetDataType(pdata.MetricDataTypeSum)
					m.Sum().SetIsMonotonic(true)
					m.Sum().SetAggregationTemporality(pdata.MetricAggregationTemporalityDelta)
					initInt64Pt(m.Sum().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("gauge_sum_double_with_dims")
					m.SetDataType(pdata.MetricDataTypeSum)
					m.Sum().SetIsMonotonic(false)
					initDoublePt(m.Sum().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("gauge_sum_int_with_dims")
					m.SetDataType(pdata.MetricDataTypeSum)
					m.Sum().SetIsMonotonic(false)
					initInt64Pt(m.Sum().DataPoints().AppendEmpty())
				}

				return out
			},
			wantSfxDataPoints: []*sfxpb.DataPoint{
				doubleSFxDataPoint("gauge_double_with_dims", &sfxMetricTypeGauge, nil),
				int64SFxDataPoint("gauge_int_with_dims", &sfxMetricTypeGauge, nil),
				doubleSFxDataPoint("cumulative_double_with_dims", &sfxMetricTypeCumulativeCounter, nil),
				int64SFxDataPoint("cumulative_int_with_dims", &sfxMetricTypeCumulativeCounter, nil),
				doubleSFxDataPoint("delta_double_with_dims", &sfxMetricTypeCounter, nil),
				int64SFxDataPoint("delta_int_with_dims", &sfxMetricTypeCounter, nil),
				doubleSFxDataPoint("gauge_sum_double_with_dims", &sfxMetricTypeGauge, nil),
				int64SFxDataPoint("gauge_sum_int_with_dims", &sfxMetricTypeGauge, nil),
			},
		},
		{
			name: "nil_node_and_resources_with_dims",
			metricsFn: func() pdata.Metrics {
				out := pdata.NewMetrics()
				ilm := out.ResourceMetrics().AppendEmpty().InstrumentationLibraryMetrics().AppendEmpty()

				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("gauge_double_with_dims")
					m.SetDataType(pdata.MetricDataTypeGauge)
					initDoublePtWithLabels(m.Gauge().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("gauge_int_with_dims")
					m.SetDataType(pdata.MetricDataTypeGauge)
					initInt64PtWithLabels(m.Gauge().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("cumulative_double_with_dims")
					m.SetDataType(pdata.MetricDataTypeSum)
					m.Sum().SetIsMonotonic(true)
					initDoublePtWithLabels(m.Sum().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("cumulative_int_with_dims")
					m.SetDataType(pdata.MetricDataTypeSum)
					m.Sum().SetIsMonotonic(true)
					initInt64PtWithLabels(m.Sum().DataPoints().AppendEmpty())
				}

				return out
			},
			wantSfxDataPoints: []*sfxpb.DataPoint{
				doubleSFxDataPoint("gauge_double_with_dims", &sfxMetricTypeGauge, labelMap),
				int64SFxDataPoint("gauge_int_with_dims", &sfxMetricTypeGauge, labelMap),
				doubleSFxDataPoint("cumulative_double_with_dims", &sfxMetricTypeCumulativeCounter, labelMap),
				int64SFxDataPoint("cumulative_int_with_dims", &sfxMetricTypeCumulativeCounter, labelMap),
			},
		},
		{
			name: "with_node_resources_dims",
			metricsFn: func() pdata.Metrics {
				out := pdata.NewMetrics()
				rm := out.ResourceMetrics().AppendEmpty()
				res := rm.Resource()
				res.Attributes().InsertString("k/r0", "vr0")
				res.Attributes().InsertString("k/r1", "vr1")
				res.Attributes().InsertString("k/n0", "vn0")
				res.Attributes().InsertString("k/n1", "vn1")

				ilm := rm.InstrumentationLibraryMetrics().AppendEmpty()
				ilm.Metrics().EnsureCapacity(2)

				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("gauge_double_with_dims")
					m.SetDataType(pdata.MetricDataTypeGauge)
					initDoublePtWithLabels(m.Gauge().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("gauge_int_with_dims")
					m.SetDataType(pdata.MetricDataTypeGauge)
					initInt64PtWithLabels(m.Gauge().DataPoints().AppendEmpty())
				}

				return out
			},
			wantSfxDataPoints: []*sfxpb.DataPoint{
				doubleSFxDataPoint(
					"gauge_double_with_dims",
					&sfxMetricTypeGauge,
					maps.MergeStringMaps(map[string]string{
						"k_n0": "vn0",
						"k_n1": "vn1",
						"k_r0": "vr0",
						"k_r1": "vr1",
					}, labelMap)),
				int64SFxDataPoint(
					"gauge_int_with_dims",
					&sfxMetricTypeGauge,
					maps.MergeStringMaps(map[string]string{
						"k_n0": "vn0",
						"k_n1": "vn1",
						"k_r0": "vr0",
						"k_r1": "vr1",
					}, labelMap)),
			},
		},
		{
			name: "with_node_resources_dims - long metric name",
			metricsFn: func() pdata.Metrics {
				out := pdata.NewMetrics()
				rm := out.ResourceMetrics().AppendEmpty()
				res := rm.Resource()
				res.Attributes().InsertString("k/r0", "vr0")
				res.Attributes().InsertString("k/r1", "vr1")
				res.Attributes().InsertString("k/n0", "vn0")
				res.Attributes().InsertString("k/n1", "vn1")

				ilm := rm.InstrumentationLibraryMetrics().AppendEmpty()
				ilm.Metrics().EnsureCapacity(5)

				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName(fmt.Sprintf("l%sng_name", strings.Repeat("o", 256)))
					m.SetDataType(pdata.MetricDataTypeGauge)
					initDoublePtWithLabels(m.Gauge().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("gauge_int_with_dims")
					m.SetDataType(pdata.MetricDataTypeGauge)
					initInt64PtWithLabels(m.Gauge().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName(fmt.Sprintf("l%sng_name", strings.Repeat("o", 256)))
					m.SetDataType(pdata.MetricDataTypeGauge)
					initDoublePtWithLabels(m.Gauge().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName(fmt.Sprintf("l%sng_name", strings.Repeat("o", 256)))
					m.SetDataType(pdata.MetricDataTypeGauge)
					initDoublePtWithLabels(m.Gauge().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("gauge_int_with_dims")
					m.SetDataType(pdata.MetricDataTypeGauge)
					initInt64PtWithLabels(m.Gauge().DataPoints().AppendEmpty())
				}

				return out
			},
			wantSfxDataPoints: []*sfxpb.DataPoint{
				int64SFxDataPoint(
					"gauge_int_with_dims",
					&sfxMetricTypeGauge,
					maps.MergeStringMaps(map[string]string{
						"k_n0": "vn0",
						"k_n1": "vn1",
						"k_r0": "vr0",
						"k_r1": "vr1",
					}, labelMap)),
				int64SFxDataPoint(
					"gauge_int_with_dims",
					&sfxMetricTypeGauge,
					maps.MergeStringMaps(map[string]string{
						"k_n0": "vn0",
						"k_n1": "vn1",
						"k_r0": "vr0",
						"k_r1": "vr1",
					}, labelMap)),
			},
		},
		{
			name: "with_node_resources_dims - long dimension name/value",
			metricsFn: func() pdata.Metrics {
				out := pdata.NewMetrics()
				rm := out.ResourceMetrics().AppendEmpty()
				res := rm.Resource()
				res.Attributes().InsertString("k/r0", "vr0")
				res.Attributes().InsertString("k/r1", "vr1")
				res.Attributes().InsertString("k/n0", "vn0")
				res.Attributes().InsertString("k/n1", "vn1")

				ilm := rm.InstrumentationLibraryMetrics().AppendEmpty()
				ilm.Metrics().EnsureCapacity(1)

				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("gauge_double_with_dims")
					m.SetDataType(pdata.MetricDataTypeGauge)
					initDoublePtWithLongLabels(m.Gauge().DataPoints().AppendEmpty())
				}

				return out
			},
			wantSfxDataPoints: []*sfxpb.DataPoint{
				doubleSFxDataPoint(
					"gauge_double_with_dims",
					&sfxMetricTypeGauge,
					maps.MergeStringMaps(map[string]string{
						"k_n0": "vn0",
						"k_n1": "vn1",
						"k_r0": "vr0",
						"k_r1": "vr1",
					}, map[string]string{
						"k0": "v0",
						"k2": "v2",
					})),
			},
		},
		{
			name: "with_resources_cloud_partial_aws_dim",
			metricsFn: func() pdata.Metrics {
				out := pdata.NewMetrics()
				rm := out.ResourceMetrics().AppendEmpty()
				res := rm.Resource()
				res.Attributes().InsertString("k/r0", "vr0")
				res.Attributes().InsertString("k/r1", "vr1")
				res.Attributes().InsertString("cloud.provider", conventions.AttributeCloudProviderAWS)
				res.Attributes().InsertString("cloud.account.id", "efgh")
				res.Attributes().InsertString("cloud.region", "us-east")

				ilm := rm.InstrumentationLibraryMetrics().AppendEmpty()
				m := ilm.Metrics().AppendEmpty()
				m.SetName("gauge_double_with_dims")
				m.SetDataType(pdata.MetricDataTypeGauge)
				initDoublePtWithLabels(m.Gauge().DataPoints().AppendEmpty())

				return out
			},
			wantSfxDataPoints: []*sfxpb.DataPoint{
				doubleSFxDataPoint(
					"gauge_double_with_dims",
					&sfxMetricTypeGauge,
					maps.MergeStringMaps(labelMap, map[string]string{
						"cloud_account_id": "efgh",
						"cloud_provider":   conventions.AttributeCloudProviderAWS,
						"cloud_region":     "us-east",
						"k_r0":             "vr0",
						"k_r1":             "vr1",
					})),
			},
		},
		{
			name: "with_resources_cloud_aws_dim",
			metricsFn: func() pdata.Metrics {
				out := pdata.NewMetrics()
				rm := out.ResourceMetrics().AppendEmpty()
				res := rm.Resource()
				res.Attributes().InsertString("k/r0", "vr0")
				res.Attributes().InsertString("k/r1", "vr1")
				res.Attributes().InsertString("cloud.provider", conventions.AttributeCloudProviderAWS)
				res.Attributes().InsertString("cloud.account.id", "efgh")
				res.Attributes().InsertString("cloud.region", "us-east")
				res.Attributes().InsertString("host.id", "abcd")

				ilm := rm.InstrumentationLibraryMetrics().AppendEmpty()
				m := ilm.Metrics().AppendEmpty()
				m.SetName("gauge_double_with_dims")
				m.SetDataType(pdata.MetricDataTypeGauge)
				initDoublePtWithLabels(m.Gauge().DataPoints().AppendEmpty())

				return out
			},
			wantSfxDataPoints: []*sfxpb.DataPoint{
				doubleSFxDataPoint(
					"gauge_double_with_dims",
					&sfxMetricTypeGauge,
					maps.MergeStringMaps(labelMap, map[string]string{
						"cloud_provider":   conventions.AttributeCloudProviderAWS,
						"cloud_account_id": "efgh",
						"cloud_region":     "us-east",
						"host_id":          "abcd",
						"AWSUniqueId":      "abcd_us-east_efgh",
						"k_r0":             "vr0",
						"k_r1":             "vr1",
					})),
			},
		},
		{
			name: "with_resources_cloud_gcp_dim_partial",
			metricsFn: func() pdata.Metrics {
				out := pdata.NewMetrics()
				rm := out.ResourceMetrics().AppendEmpty()
				res := rm.Resource()
				res.Attributes().InsertString("k/r0", "vr0")
				res.Attributes().InsertString("k/r1", "vr1")
				res.Attributes().InsertString("cloud.provider", conventions.AttributeCloudProviderGCP)
				res.Attributes().InsertString("host.id", "abcd")

				ilm := rm.InstrumentationLibraryMetrics().AppendEmpty()
				m := ilm.Metrics().AppendEmpty()
				m.SetName("gauge_double_with_dims")
				m.SetDataType(pdata.MetricDataTypeGauge)
				initDoublePtWithLabels(m.Gauge().DataPoints().AppendEmpty())

				return out
			},
			wantSfxDataPoints: []*sfxpb.DataPoint{
				doubleSFxDataPoint(
					"gauge_double_with_dims",
					&sfxMetricTypeGauge,
					maps.MergeStringMaps(labelMap, map[string]string{
						"host_id":        "abcd",
						"cloud_provider": conventions.AttributeCloudProviderGCP,
						"k_r0":           "vr0",
						"k_r1":           "vr1",
					})),
			},
		},
		{
			name: "with_resources_cloud_gcp_dim",
			metricsFn: func() pdata.Metrics {
				out := pdata.NewMetrics()
				rm := out.ResourceMetrics().AppendEmpty()
				res := rm.Resource()
				res.Attributes().InsertString("k/r0", "vr0")
				res.Attributes().InsertString("k/r1", "vr1")
				res.Attributes().InsertString("cloud.provider", conventions.AttributeCloudProviderGCP)
				res.Attributes().InsertString("host.id", "abcd")
				res.Attributes().InsertString("cloud.account.id", "efgh")

				ilm := rm.InstrumentationLibraryMetrics().AppendEmpty()
				m := ilm.Metrics().AppendEmpty()
				m.SetName("gauge_double_with_dims")
				m.SetDataType(pdata.MetricDataTypeGauge)
				initDoublePtWithLabels(m.Gauge().DataPoints().AppendEmpty())

				return out
			},
			wantSfxDataPoints: []*sfxpb.DataPoint{
				doubleSFxDataPoint(
					"gauge_double_with_dims",
					&sfxMetricTypeGauge,
					maps.MergeStringMaps(labelMap, map[string]string{
						"gcp_id":           "efgh_abcd",
						"k_r0":             "vr0",
						"k_r1":             "vr1",
						"cloud_provider":   conventions.AttributeCloudProviderGCP,
						"host_id":          "abcd",
						"cloud_account_id": "efgh",
					})),
			},
		},
		{
			name: "with_exclude_metrics_filter",
			metricsFn: func() pdata.Metrics {
				out := pdata.NewMetrics()
				ilm := out.ResourceMetrics().AppendEmpty().InstrumentationLibraryMetrics().AppendEmpty()

				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("gauge_double_with_dims")
					m.SetDataType(pdata.MetricDataTypeGauge)
					initDoublePtWithLabels(m.Gauge().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("gauge_int_with_dims")
					m.SetDataType(pdata.MetricDataTypeGauge)
					initInt64PtWithLabels(m.Gauge().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("cumulative_double_with_dims")
					m.SetDataType(pdata.MetricDataTypeSum)
					m.Sum().SetIsMonotonic(true)
					initDoublePtWithLabels(m.Sum().DataPoints().AppendEmpty())
					initDoublePtWithDifferentLabels(m.Sum().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("cumulative_int_with_dims")
					m.SetDataType(pdata.MetricDataTypeSum)
					m.Sum().SetIsMonotonic(true)
					initInt64PtWithLabels(m.Sum().DataPoints().AppendEmpty())
				}

				return out
			},
			excludeMetrics: []dpfilters.MetricFilter{
				{
					MetricNames: []string{"gauge_double_with_dims"},
				},
				{
					MetricName: "cumulative_int_with_dims",
				},
				{
					MetricName: "gauge_int_with_dims",
					Dimensions: map[string]interface{}{
						"k0": []interface{}{"v1"},
					},
				},
				{
					MetricName: "cumulative_double_with_dims",
					Dimensions: map[string]interface{}{
						"k0": []interface{}{"v0"},
					},
				},
			},
			wantSfxDataPoints: []*sfxpb.DataPoint{
				int64SFxDataPoint("gauge_int_with_dims", &sfxMetricTypeGauge, labelMap),
				doubleSFxDataPoint("cumulative_double_with_dims", &sfxMetricTypeCumulativeCounter, differentLabelMap),
			},
		},
		{
			// To validate that filters in include serves as override to the ones in exclude list.
			name: "with_include_and_exclude_metrics_filter",
			metricsFn: func() pdata.Metrics {
				out := pdata.NewMetrics()
				ilm := out.ResourceMetrics().AppendEmpty().InstrumentationLibraryMetrics().AppendEmpty()

				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("gauge_double_with_dims")
					m.SetDataType(pdata.MetricDataTypeGauge)
					initDoublePtWithLabels(m.Gauge().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("gauge_int_with_dims")
					m.SetDataType(pdata.MetricDataTypeGauge)
					initInt64PtWithLabels(m.Gauge().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("cumulative_double_with_dims")
					m.SetDataType(pdata.MetricDataTypeSum)
					m.Sum().SetIsMonotonic(true)
					initDoublePtWithLabels(m.Sum().DataPoints().AppendEmpty())
					initDoublePtWithDifferentLabels(m.Sum().DataPoints().AppendEmpty())
				}
				{
					m := ilm.Metrics().AppendEmpty()
					m.SetName("cumulative_int_with_dims")
					m.SetDataType(pdata.MetricDataTypeSum)
					m.Sum().SetIsMonotonic(true)
					initInt64PtWithLabels(m.Sum().DataPoints().AppendEmpty())
				}

				return out
			},
			excludeMetrics: []dpfilters.MetricFilter{
				{
					MetricNames: []string{"gauge_double_with_dims"},
				},
				{
					MetricName: "cumulative_int_with_dims",
				},
				{
					MetricName: "gauge_int_with_dims",
					Dimensions: map[string]interface{}{
						"k0": []interface{}{"v1"},
					},
				},
				{
					MetricName: "cumulative_double_with_dims",
					Dimensions: map[string]interface{}{
						"k0": []interface{}{"v0"},
					},
				},
			},
			includeMetrics: []dpfilters.MetricFilter{
				{
					MetricName: "cumulative_int_with_dims",
				},
			},
			wantSfxDataPoints: []*sfxpb.DataPoint{
				int64SFxDataPoint("gauge_int_with_dims", &sfxMetricTypeGauge, labelMap),
				doubleSFxDataPoint("cumulative_double_with_dims", &sfxMetricTypeCumulativeCounter, differentLabelMap),
				int64SFxDataPoint("cumulative_int_with_dims", &sfxMetricTypeCumulativeCounter, labelMap),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewMetricsConverter(logger, nil, tt.excludeMetrics, tt.includeMetrics, "")
			require.NoError(t, err)
			md := tt.metricsFn()
			gotSfxDataPoints := c.MetricsToSignalFxV2(md)
			// Sort SFx dimensions since they are built from maps and the order
			// of those is not deterministic.
			sortDimensions(tt.wantSfxDataPoints)
			sortDimensions(gotSfxDataPoints)
			assert.Equal(t, tt.wantSfxDataPoints, gotSfxDataPoints)
		})
	}
}

func TestMetricDataToSignalFxV2WithTranslation(t *testing.T) {
	translator, err := NewMetricTranslator([]Rule{
		{
			Action: ActionRenameDimensionKeys,
			Mapping: map[string]string{
				"old.dim": "new.dim",
			},
		},
	}, 1)
	require.NoError(t, err)

	md := pdata.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().InstrumentationLibraryMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetDataType(pdata.MetricDataTypeGauge)
	m.SetName("metric1")
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.SetIntVal(123)
	dp.Attributes().InsertString("old.dim", "val1")

	gaugeType := sfxpb.MetricType_GAUGE
	expected := []*sfxpb.DataPoint{
		{
			Metric: "metric1",
			Value: sfxpb.Datum{
				IntValue: generateIntPtr(123),
			},
			MetricType: &gaugeType,
			Dimensions: []*sfxpb.Dimension{
				{
					Key:   "new_dim",
					Value: "val1",
				},
			},
		},
	}
	c, err := NewMetricsConverter(zap.NewNop(), translator, nil, nil, "")
	require.NoError(t, err)
	assert.EqualValues(t, expected, c.MetricsToSignalFxV2(md))
}

func TestDimensionKeyCharsWithPeriod(t *testing.T) {
	translator, err := NewMetricTranslator([]Rule{
		{
			Action: ActionRenameDimensionKeys,
			Mapping: map[string]string{
				"old.dim.with.periods": "new.dim.with.periods",
			},
		},
	}, 1)
	require.NoError(t, err)

	md := pdata.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().InstrumentationLibraryMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetDataType(pdata.MetricDataTypeGauge)
	m.SetName("metric1")
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.SetIntVal(123)
	dp.Attributes().InsertString("old.dim.with.periods", "val1")

	gaugeType := sfxpb.MetricType_GAUGE
	expected := []*sfxpb.DataPoint{
		{
			Metric: "metric1",
			Value: sfxpb.Datum{
				IntValue: generateIntPtr(123),
			},
			MetricType: &gaugeType,
			Dimensions: []*sfxpb.Dimension{
				{
					Key:   "new.dim.with.periods",
					Value: "val1",
				},
			},
		},
	}
	c, err := NewMetricsConverter(zap.NewNop(), translator, nil, nil, "_-.")
	require.NoError(t, err)
	assert.EqualValues(t, expected, c.MetricsToSignalFxV2(md))

}

func sortDimensions(points []*sfxpb.DataPoint) {
	for _, point := range points {
		if point.Dimensions == nil {
			continue
		}
		sort.Slice(point.Dimensions, func(i, j int) bool {
			return point.Dimensions[i].Key < point.Dimensions[j].Key
		})
	}
}

func doubleSFxDataPoint(
	metric string,
	metricType *sfxpb.MetricType,
	dims map[string]string,
) *sfxpb.DataPoint {
	return &sfxpb.DataPoint{
		Metric:     metric,
		Timestamp:  tsMSecs,
		Value:      sfxpb.Datum{DoubleValue: &doubleVal},
		MetricType: metricType,
		Dimensions: sfxDimensions(dims),
	}
}

func int64SFxDataPoint(
	metric string,
	metricType *sfxpb.MetricType,
	dims map[string]string,
) *sfxpb.DataPoint {
	return &sfxpb.DataPoint{
		Metric:     metric,
		Timestamp:  tsMSecs,
		Value:      sfxpb.Datum{IntValue: &int64Val},
		MetricType: metricType,
		Dimensions: sfxDimensions(dims),
	}
}

func sfxDimensions(m map[string]string) []*sfxpb.Dimension {
	sfxDims := make([]*sfxpb.Dimension, 0, len(m))
	for k, v := range m {
		sfxDims = append(sfxDims, &sfxpb.Dimension{
			Key:   k,
			Value: v,
		})
	}

	return sfxDims
}

func TestNewMetricsConverter(t *testing.T) {
	tests := []struct {
		name     string
		excludes []dpfilters.MetricFilter
		want     *MetricsConverter
		wantErr  bool
	}{
		{
			name:     "Error on creating filterSet",
			excludes: []dpfilters.MetricFilter{{}},
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMetricsConverter(zap.NewNop(), nil, tt.excludes, nil, "")
			if (err != nil) != tt.wantErr {
				t.Errorf("NewMetricsConverter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewMetricsConverter() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMetricsConverter_ConvertDimension(t *testing.T) {
	type fields struct {
		metricTranslator        *MetricTranslator
		nonAlphanumericDimChars string
	}
	type args struct {
		dim string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "No translations",
			fields: fields{
				metricTranslator:        nil,
				nonAlphanumericDimChars: "_-",
			},
			args: args{
				dim: "d.i.m",
			},
			want: "d_i_m",
		},
		{
			name: "With translations",
			fields: fields{
				metricTranslator: func() *MetricTranslator {
					t, _ := NewMetricTranslator([]Rule{
						{
							Action: ActionRenameDimensionKeys,
							Mapping: map[string]string{
								"d.i.m": "di.m",
							},
						},
					}, 0)
					return t
				}(),
				nonAlphanumericDimChars: "_-",
			},
			args: args{
				dim: "d.i.m",
			},
			want: "di_m",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewMetricsConverter(zap.NewNop(), tt.fields.metricTranslator, nil, nil, tt.fields.nonAlphanumericDimChars)
			require.NoError(t, err)
			if got := c.ConvertDimension(tt.args.dim); got != tt.want {
				t.Errorf("ConvertDimension() = %v, want %v", got, tt.want)
			}
		})
	}
}

func stringMapToAttributeMap(m map[string]string) map[string]pdata.AttributeValue {
	ret := map[string]pdata.AttributeValue{}
	for k, v := range m {
		ret[k] = pdata.NewAttributeValueString(v)
	}
	return ret
}
