// Copyright The OpenTelemetry Authors
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

package metricsgenerationprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/model/pdata"
)

type testMetric struct {
	metricNames  []string
	metricValues [][]float64
}

type testMetricIntGauge struct {
	metricNames  []string
	metricValues [][]int64
}

type metricsGenerationTest struct {
	name       string
	rules      []Rule
	inMetrics  pdata.Metrics
	outMetrics pdata.Metrics
}

var (
	testCases = []metricsGenerationTest{
		{
			name:  "metrics_generation_expect_all",
			rules: nil,
			inMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2"},
				metricValues: [][]float64{{100}, {4}},
			}),
			outMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2"},
				metricValues: [][]float64{{100}, {4}},
			}),
		},
		{
			name: "metrics_generation_rule_scale",
			rules: []Rule{
				{
					Name:      "metric_1_scaled",
					Type:      "scale",
					Metric1:   "metric_1",
					Operation: "multiply",
					ScaleBy:   5,
				},
			},
			inMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2"},
				metricValues: [][]float64{{100}, {4}},
			}),
			outMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2", "metric_1_scaled"},
				metricValues: [][]float64{{100}, {4}, {500}},
			}),
		},
		{
			name: "metrics_generation_missing_first_metric",
			rules: []Rule{
				{
					Name:      "metric_1_scaled",
					Type:      "scale",
					Operation: "multiply",
					ScaleBy:   5,
				},
			},
			inMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2"},
				metricValues: [][]float64{{100}, {4}},
			}),
			outMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2"},
				metricValues: [][]float64{{100}, {4}},
			}),
		},
		{
			name: "metrics_generation_rule_calculate_divide",
			rules: []Rule{
				{
					Name:      "metric_1_calculated_divide",
					Type:      "calculate",
					Metric1:   "metric_1",
					Metric2:   "metric_2",
					Operation: "divide",
				},
			},
			inMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2"},
				metricValues: [][]float64{{100}, {4}},
			}),
			outMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2", "metric_1_calculated_divide"},
				metricValues: [][]float64{{100}, {4}, {25}},
			}),
		},
		{
			name: "metrics_generation_rule_calculate_multiply",
			rules: []Rule{
				{
					Name:      "metric_1_calculated_multiply",
					Type:      "calculate",
					Metric1:   "metric_1",
					Metric2:   "metric_2",
					Operation: "multiply",
				},
			},
			inMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2"},
				metricValues: [][]float64{{100}, {4}},
			}),
			outMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2", "metric_1_calculated_multiply"},
				metricValues: [][]float64{{100}, {4}, {400}},
			}),
		},
		{
			name: "metrics_generation_rule_calculate_add",
			rules: []Rule{
				{
					Name:      "metric_1_calculated_add",
					Type:      "calculate",
					Metric1:   "metric_1",
					Metric2:   "metric_2",
					Operation: "add",
				},
			},
			inMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2"},
				metricValues: [][]float64{{100}, {4}},
			}),
			outMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2", "metric_1_calculated_add"},
				metricValues: [][]float64{{100}, {4}, {104}},
			}),
		},
		{
			name: "metrics_generation_rule_calculate_subtract",
			rules: []Rule{
				{
					Name:      "metric_1_calculated_subtract",
					Type:      "calculate",
					Metric1:   "metric_1",
					Metric2:   "metric_2",
					Operation: "subtract",
				},
			},
			inMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2"},
				metricValues: [][]float64{{100}, {4}},
			}),
			outMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2", "metric_1_calculated_subtract"},
				metricValues: [][]float64{{100}, {4}, {96}},
			}),
		},
		{
			name: "metrics_generation_rule_calculate_percent",
			rules: []Rule{
				{
					Name:      "metric_1_calculated_percent",
					Type:      "calculate",
					Metric1:   "metric_1",
					Metric2:   "metric_2",
					Operation: "percent",
				},
			},
			inMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2"},
				metricValues: [][]float64{{20}, {200}},
			}),
			outMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2", "metric_1_calculated_percent"},
				metricValues: [][]float64{{20}, {200}, {10}},
			}),
		},
		{
			name: "metrics_generation_rule_calculate_missing_2nd_metric",
			rules: []Rule{
				{
					Name:      "metric_1_calculated_multiply",
					Type:      "calculate",
					Metric1:   "metric_1",
					Operation: "multiply",
				},
			},
			inMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2"},
				metricValues: [][]float64{{100}, {4}},
			}),
			outMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2"},
				metricValues: [][]float64{{100}, {4}},
			}),
		},
		{
			name: "metrics_generation_rule_calculate_divide_op2_zero",
			rules: []Rule{
				{
					Name:      "metric_1_calculated_divide",
					Type:      "calculate",
					Metric1:   "metric_1",
					Metric2:   "metric_2",
					Operation: "divide",
				},
			},
			inMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2"},
				metricValues: [][]float64{{100}, {0}},
			}),
			outMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2"},
				metricValues: [][]float64{{100}, {0}},
			}),
		},
		{
			name: "metrics_generation_rule_calculate_invalid_operation",
			rules: []Rule{
				{
					Name:      "metric_1_calculated_invalid",
					Type:      "calculate",
					Metric1:   "metric_1",
					Metric2:   "metric_2",
					Operation: "invalid",
				},
			},
			inMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2"},
				metricValues: [][]float64{{100}, {0}},
			}),
			outMetrics: generateTestMetrics(testMetric{
				metricNames:  []string{"metric_1", "metric_2"},
				metricValues: [][]float64{{100}, {0}},
			}),
		},
		{
			name: "metrics_generation_test_int_gauge_add",
			rules: []Rule{
				{
					Name:      "metric_calculated",
					Type:      "calculate",
					Metric1:   "metric_1",
					Metric2:   "metric_2",
					Operation: "add",
				},
			},
			inMetrics: generateTestMetricsWithIntDatapoint(testMetricIntGauge{
				metricNames:  []string{"metric_1", "metric_2"},
				metricValues: [][]int64{{100}, {5}},
			}),
			outMetrics: getOutputForIntGaugeTest(),
		},
	}
)

func TestMetricsGenerationProcessor(t *testing.T) {
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			// next stores the results of the filter metric processor
			next := new(consumertest.MetricsSink)
			cfg := &Config{
				ProcessorSettings: config.NewProcessorSettings(config.NewComponentID(typeStr)),
				Rules:             test.rules,
			}
			factory := NewFactory()
			mgp, err := factory.CreateMetricsProcessor(
				context.Background(),
				componenttest.NewNopProcessorCreateSettings(),
				cfg,
				next,
			)
			assert.NotNil(t, mgp)
			assert.Nil(t, err)

			caps := mgp.Capabilities()
			assert.True(t, caps.MutatesData)
			ctx := context.Background()
			require.NoError(t, mgp.Start(ctx, nil))

			cErr := mgp.ConsumeMetrics(context.Background(), test.inMetrics)
			assert.Nil(t, cErr)
			got := next.AllMetrics()

			require.Equal(t, 1, len(got))
			require.Equal(t, test.outMetrics.ResourceMetrics().Len(), got[0].ResourceMetrics().Len())

			expectedMetrics := test.outMetrics.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics()
			actualMetrics := got[0].ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics()

			require.Equal(t, expectedMetrics.Len(), actualMetrics.Len())

			for i := 0; i < expectedMetrics.Len(); i++ {
				eM := expectedMetrics.At(i)
				aM := actualMetrics.At(i)

				require.Equal(t, eM.Name(), aM.Name())

				if eM.DataType() == pdata.MetricDataTypeGauge {
					eDataPoints := eM.Gauge().DataPoints()
					aDataPoints := aM.Gauge().DataPoints()
					require.Equal(t, eDataPoints.Len(), aDataPoints.Len())

					for j := 0; j < eDataPoints.Len(); j++ {
						switch eDataPoints.At(j).ValueType() {
						case pdata.MetricValueTypeDouble:
							require.Equal(t, eDataPoints.At(j).DoubleVal(), aDataPoints.At(j).DoubleVal())
						case pdata.MetricValueTypeInt:
							require.Equal(t, eDataPoints.At(j).IntVal(), aDataPoints.At(j).IntVal())
						}

					}
				}

			}

			require.NoError(t, mgp.Shutdown(ctx))
		})
	}
}

func generateTestMetrics(tm testMetric) pdata.Metrics {
	md := pdata.NewMetrics()
	now := time.Now()

	rm := md.ResourceMetrics().AppendEmpty()
	ms := rm.InstrumentationLibraryMetrics().AppendEmpty().Metrics()
	for i, name := range tm.metricNames {
		m := ms.AppendEmpty()
		m.SetName(name)
		m.SetDataType(pdata.MetricDataTypeGauge)
		for _, value := range tm.metricValues[i] {
			dp := m.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(pdata.NewTimestampFromTime(now.Add(10 * time.Second)))
			dp.SetDoubleVal(value)
		}
	}

	return md
}

func generateTestMetricsWithIntDatapoint(tm testMetricIntGauge) pdata.Metrics {
	md := pdata.NewMetrics()
	now := time.Now()

	rm := md.ResourceMetrics().AppendEmpty()
	ms := rm.InstrumentationLibraryMetrics().AppendEmpty().Metrics()
	for i, name := range tm.metricNames {
		m := ms.AppendEmpty()
		m.SetName(name)
		m.SetDataType(pdata.MetricDataTypeGauge)
		for _, value := range tm.metricValues[i] {
			dp := m.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(pdata.NewTimestampFromTime(now.Add(10 * time.Second)))
			dp.SetIntVal(value)
		}
	}

	return md
}

func getOutputForIntGaugeTest() pdata.Metrics {
	intGaugeOutputMetrics := generateTestMetricsWithIntDatapoint(testMetricIntGauge{
		metricNames:  []string{"metric_1", "metric_2"},
		metricValues: [][]int64{{100}, {5}},
	})
	ilm := intGaugeOutputMetrics.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics()
	doubleMetric := ilm.AppendEmpty()
	doubleMetric.SetDataType(pdata.MetricDataTypeGauge)
	doubleMetric.SetName("metric_calculated")
	neweDoubleDataPoint := doubleMetric.Gauge().DataPoints().AppendEmpty()
	neweDoubleDataPoint.SetDoubleVal(105)

	return intGaugeOutputMetrics
}
