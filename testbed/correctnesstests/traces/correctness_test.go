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

package traces

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector-contrib/testbed/correctnesstests"
	"github.com/open-telemetry/opentelemetry-collector-contrib/testbed/testbed"
)

var correctnessResults testbed.TestResultsSummary = &testbed.CorrectnessResults{}

func TestMain(m *testing.M) {
	testbed.DoTestMain(m, correctnessResults)
}

func TestTracingGoldenData(t *testing.T) {
	tests, err := correctnesstests.LoadPictOutputPipelineDefs("testdata/generated_pict_pairs_traces_pipeline.txt")
	require.NoError(t, err)
	processors := map[string]string{
		"batch": `
  batch:
    send_batch_size: 1024
`,
	}
	for _, test := range tests {
		test.TestName = fmt.Sprintf("%s-%s", test.Receiver, test.Exporter)
		test.DataSender = correctnesstests.ConstructTraceSender(t, test.Receiver)
		test.DataReceiver = correctnesstests.ConstructReceiver(t, test.Exporter)
		t.Run(test.TestName, func(t *testing.T) {
			testWithTracingGoldenDataset(t, test.DataSender, test.DataReceiver, test.ResourceSpec, processors)
		})
	}
}

func testWithTracingGoldenDataset(
	t *testing.T,
	sender testbed.DataSender,
	receiver testbed.DataReceiver,
	resourceSpec testbed.ResourceSpec,
	processors map[string]string,
) {
	dataProvider := testbed.NewGoldenDataProvider(
		"../../../internal/coreinternal/goldendataset/testdata/generated_pict_pairs_traces.txt",
		"../../../internal/coreinternal/goldendataset/testdata/generated_pict_pairs_spans.txt",
		"")
	factories, err := testbed.Components()
	require.NoError(t, err, "default components resulted in: %v", err)
	runner := testbed.NewInProcessCollector(factories)
	validator := testbed.NewCorrectTestValidator(sender.ProtocolName(), receiver.ProtocolName(), dataProvider)
	config := correctnesstests.CreateConfigYaml(sender, receiver, processors, "traces")
	log.Println(config)
	configCleanup, cfgErr := runner.PrepareConfig(config)
	require.NoError(t, cfgErr, "collector configuration resulted in: %v", cfgErr)
	defer configCleanup()
	tc := testbed.NewTestCase(
		t,
		dataProvider,
		sender,
		receiver,
		runner,
		validator,
		correctnessResults,
		testbed.WithResourceLimits(resourceSpec),
	)
	defer tc.Stop()

	tc.EnableRecording()
	tc.StartBackend()
	tc.StartAgent()

	tc.StartLoad(testbed.LoadOptions{
		DataItemsPerSecond: 1024,
		ItemsPerBatch:      1,
	})

	tc.Sleep(2 * time.Second)

	tc.StopLoad()

	tc.WaitForN(func() bool { return tc.LoadGenerator.DataItemsSent() == tc.MockBackend.DataItemsReceived() },
		3*time.Second, "all data items received")

	tc.StopAgent()

	tc.ValidateData()
}
