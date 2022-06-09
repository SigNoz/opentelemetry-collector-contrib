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

package tests

// This file contains Test functions which initiate the tests. The tests can be either
// coded in this file or use scenarios from perf_scenarios.go.

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config"

	"github.com/open-telemetry/opentelemetry-collector-contrib/testbed/datareceivers"
	"github.com/open-telemetry/opentelemetry-collector-contrib/testbed/datasenders"
	"github.com/open-telemetry/opentelemetry-collector-contrib/testbed/testbed"
)

func TestMetric10kDPS(t *testing.T) {
	tests := []struct {
		name         string
		sender       testbed.DataSender
		receiver     testbed.DataReceiver
		resourceSpec testbed.ResourceSpec
	}{
		{
			"Carbon",
			datasenders.NewCarbonDataSender(testbed.GetAvailablePort(t)),
			datareceivers.NewCarbonDataReceiver(testbed.GetAvailablePort(t)),
			testbed.ResourceSpec{
				ExpectedMaxCPU: 237,
				ExpectedMaxRAM: 100,
			},
		},
		{
			"OpenCensus",
			datasenders.NewOCMetricDataSender(testbed.DefaultHost, testbed.GetAvailablePort(t)),
			datareceivers.NewOCDataReceiver(testbed.GetAvailablePort(t)),
			testbed.ResourceSpec{
				ExpectedMaxCPU: 85,
				ExpectedMaxRAM: 100,
			},
		},
		{
			"OTLP",
			testbed.NewOTLPMetricDataSender(testbed.DefaultHost, testbed.GetAvailablePort(t)),
			testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
			testbed.ResourceSpec{
				ExpectedMaxCPU: 50,
				ExpectedMaxRAM: 98,
			},
		},
		{
			"OTLP-HTTP",
			testbed.NewOTLPHTTPMetricDataSender(testbed.DefaultHost, testbed.GetAvailablePort(t)),
			testbed.NewOTLPHTTPDataReceiver(testbed.GetAvailablePort(t)),
			testbed.ResourceSpec{
				ExpectedMaxCPU: 55,
				ExpectedMaxRAM: 92,
			},
		},
		{
			"SignalFx",
			datasenders.NewSFxMetricDataSender(testbed.GetAvailablePort(t)),
			datareceivers.NewSFxMetricsDataReceiver(testbed.GetAvailablePort(t)),
			testbed.ResourceSpec{
				ExpectedMaxCPU: 120,
				ExpectedMaxRAM: 98,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			Scenario10kItemsPerSecond(
				t,
				test.sender,
				test.receiver,
				test.resourceSpec,
				performanceResultsSummary,
				nil,
				nil,
			)
		})
	}

}

func TestMetricsFromFile(t *testing.T) {
	// This test demonstrates usage of NewFileDataProvider to generate load using
	// previously recorded data.

	resultDir, err := filepath.Abs(filepath.Join("results", t.Name()))
	require.NoError(t, err)

	// Use metrics previously recorded using "file" exporter and "k8scluster" receiver.
	dataProvider, err := testbed.NewFileDataProvider("testdata/k8s-metrics.json", config.MetricsDataType)
	assert.NoError(t, err)

	options := testbed.LoadOptions{
		DataItemsPerSecond: 1_000,
		Parallel:           1,
		// ItemsPerBatch is based on the data from the file.
		ItemsPerBatch: dataProvider.ItemsPerBatch,
	}
	agentProc := testbed.NewChildProcessCollector()

	sender := testbed.NewOTLPMetricDataSender(testbed.DefaultHost, testbed.GetAvailablePort(t))
	receiver := testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t))

	configStr := createConfigYaml(t, sender, receiver, resultDir, nil, nil)
	configCleanup, err := agentProc.PrepareConfig(configStr)
	require.NoError(t, err)
	defer configCleanup()

	tc := testbed.NewTestCase(
		t,
		dataProvider,
		sender,
		receiver,
		agentProc,
		&testbed.PerfTestValidator{},
		performanceResultsSummary,
		testbed.WithResourceLimits(testbed.ResourceSpec{ExpectedMaxCPU: 120, ExpectedMaxRAM: 94}),
	)
	defer tc.Stop()

	tc.StartBackend()
	tc.StartAgent()

	tc.StartLoad(options)

	tc.Sleep(tc.Duration)

	tc.StopLoad()

	tc.WaitFor(func() bool { return tc.LoadGenerator.DataItemsSent() > 0 }, "load generator started")
	tc.WaitFor(func() bool { return tc.LoadGenerator.DataItemsSent() == tc.MockBackend.DataItemsReceived() },
		"all data items received")

	tc.StopAgent()

	tc.ValidateData()
}
