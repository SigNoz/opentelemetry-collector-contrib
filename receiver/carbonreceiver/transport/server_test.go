// Copyright 2019, OpenTelemetry Authors
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

package transport

import (
	"net"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/testutil"
	internaldata "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/opencensus"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/carbonreceiver/protocol"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/carbonreceiver/transport/client"
)

func Test_Server_ListenAndServe(t *testing.T) {
	tests := []struct {
		name          string
		buildServerFn func(addr string) (Server, error)
		buildClientFn func(host string, port int) (*client.Graphite, error)
	}{
		{
			name: "tcp",
			buildServerFn: func(addr string) (Server, error) {
				return NewTCPServer(addr, 1*time.Second)
			},
			buildClientFn: func(host string, port int) (*client.Graphite, error) {
				return client.NewGraphite(client.TCP, host, port)
			},
		},
		{
			name: "udp",
			buildServerFn: func(addr string) (Server, error) {
				return NewUDPServer(addr)
			},
			buildClientFn: func(host string, port int) (*client.Graphite, error) {
				return client.NewGraphite(client.UDP, host, port)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := testutil.GetAvailableLocalAddress(t)
			svr, err := tt.buildServerFn(addr)
			require.NoError(t, err)
			require.NotNil(t, svr)

			host, portStr, err := net.SplitHostPort(addr)
			require.NoError(t, err)
			port, err := strconv.Atoi(portStr)
			require.NoError(t, err)

			mc := new(consumertest.MetricsSink)
			p, err := (&protocol.PlaintextConfig{}).BuildParser()
			require.NoError(t, err)
			mr := NewMockReporter(1)

			wgListenAndServe := sync.WaitGroup{}
			wgListenAndServe.Add(1)
			go func() {
				defer wgListenAndServe.Done()
				assert.Error(t, svr.ListenAndServe(p, mc, mr))
			}()

			runtime.Gosched()

			gc, err := tt.buildClientFn(host, port)
			require.NoError(t, err)
			require.NotNil(t, gc)

			ts := time.Date(2020, 2, 20, 20, 20, 20, 20, time.UTC)
			err = gc.SendMetric(client.Metric{
				Name: "test.metric", Value: 1, Timestamp: ts})
			assert.NoError(t, err)
			runtime.Gosched()

			err = gc.Disconnect()
			assert.NoError(t, err)

			mr.WaitAllOnMetricsProcessedCalls()

			err = svr.Close()
			assert.NoError(t, err)

			wgListenAndServe.Wait()

			mdd := mc.AllMetrics()
			require.Len(t, mdd, 1)
			_, _, metrics := internaldata.ResourceMetricsToOC(mdd[0].ResourceMetrics().At(0))
			require.Len(t, metrics, 1)
			assert.Equal(t, "test.metric", metrics[0].GetMetricDescriptor().GetName())
		})
	}
}
