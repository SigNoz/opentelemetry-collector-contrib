// Copyright 2020, OpenTelemetry Authors
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

package zookeeperreceiver

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/testutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/scrapertest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/scrapertest/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/zookeeperreceiver/internal/metadata"
)

type logMsg struct {
	msg   string
	level zapcore.Level
}

func TestZookeeperMetricsScraperScrape(t *testing.T) {
	tests := []struct {
		name                         string
		expectedMetricsFilename      string
		expectedResourceAttributes   map[string]string
		metricsSettings              func() metadata.MetricsSettings
		mockedZKOutputSourceFilename string
		mockZKConnectionErr          bool
		expectedLogs                 []logMsg
		expectedNumResourceMetrics   int
		setConnectionDeadline        func(net.Conn, time.Time) error
		closeConnection              func(net.Conn) error
		sendCmd                      func(net.Conn, string) (*bufio.Scanner, error)
		wantErr                      bool
	}{
		{
			name:                         "Test correctness with v3.4.14",
			mockedZKOutputSourceFilename: "mntr-3.4.14",
			expectedMetricsFilename:      "correctness-v3.4.14",
			expectedResourceAttributes: map[string]string{
				"server.state": "standalone",
				"zk.version":   "3.4.14-4c25d480e66aadd371de8bd2fd8da255ac140bcf",
			},
			expectedLogs: []logMsg{
				{
					msg:   "metric computation failed",
					level: zapcore.DebugLevel,
				},
			},
			expectedNumResourceMetrics: 1,
		},
		{
			name:                         "Test correctness with v3.5.5",
			mockedZKOutputSourceFilename: "mntr-3.5.5",
			expectedMetricsFilename:      "correctness-v3.5.5",
			expectedResourceAttributes: map[string]string{
				"server.state": "leader",
				"zk.version":   "3.5.5-390fe37ea45dee01bf87dc1c042b5e3dcce88653",
			},
			expectedNumResourceMetrics: 1,
		},
		{
			name:                "Arbitrary connection error",
			mockZKConnectionErr: true,
			expectedLogs: []logMsg{
				{
					msg:   "failed to establish connection",
					level: zapcore.ErrorLevel,
				},
			},
			wantErr: true,
		},
		{
			name:                         "Unexpected line format in mntr",
			mockedZKOutputSourceFilename: "mntr-unexpected_line_format",
			expectedLogs: []logMsg{
				{
					msg:   "unexpected line in response",
					level: zapcore.WarnLevel,
				},
				{
					msg:   "metric computation failed",
					level: zapcore.DebugLevel,
				},
			},
			expectedNumResourceMetrics: 0,
		},
		{
			name:                         "Unexpected value type in mntr",
			mockedZKOutputSourceFilename: "mntr-unexpected_value_type",
			expectedLogs: []logMsg{
				{
					msg:   "non-integer value from mntr",
					level: zapcore.DebugLevel,
				},
				{
					msg:   "metric computation failed",
					level: zapcore.DebugLevel,
				},
			},
			expectedNumResourceMetrics: 0,
		},
		{
			name:                         "Error setting connection deadline",
			mockedZKOutputSourceFilename: "mntr-3.4.14",
			expectedLogs: []logMsg{
				{
					msg:   "failed to set deadline on connection",
					level: zapcore.WarnLevel,
				},
				{
					msg:   "metric computation failed",
					level: zapcore.DebugLevel,
				},
			},
			expectedMetricsFilename: "error-setting-connection-deadline",
			expectedResourceAttributes: map[string]string{
				"server.state": "standalone",
				"zk.version":   "3.4.14-4c25d480e66aadd371de8bd2fd8da255ac140bcf",
			},
			expectedNumResourceMetrics: 1,
			setConnectionDeadline: func(conn net.Conn, t time.Time) error {
				return errors.New("")
			},
		},
		{
			name:                         "Error closing connection",
			mockedZKOutputSourceFilename: "mntr-3.4.14",
			expectedLogs: []logMsg{
				{
					msg:   "metric computation failed",
					level: zapcore.DebugLevel,
				},
				{
					msg:   "failed to shutdown connection",
					level: zapcore.WarnLevel,
				},
			},
			expectedMetricsFilename: "error-closing-connection",
			expectedResourceAttributes: map[string]string{
				"server.state": "standalone",
				"zk.version":   "3.4.14-4c25d480e66aadd371de8bd2fd8da255ac140bcf",
			},
			expectedNumResourceMetrics: 1,
			closeConnection: func(conn net.Conn) error {
				return errors.New("")
			},
		},
		{
			name:                         "Failed to send command",
			mockedZKOutputSourceFilename: "mntr-3.4.14",
			expectedLogs: []logMsg{
				{
					msg:   "failed to send command",
					level: zapcore.ErrorLevel,
				},
			},
			sendCmd: func(conn net.Conn, s string) (*bufio.Scanner, error) {
				return nil, errors.New("")
			},
		},
		{
			name: "Disable zookeeper.watches metric",
			metricsSettings: func() metadata.MetricsSettings {
				ms := metadata.DefaultMetricsSettings()
				ms.ZookeeperWatchCount.Enabled = false
				return ms
			},
			mockedZKOutputSourceFilename: "mntr-3.4.14",
			expectedMetricsFilename:      "disable-watches",
			expectedResourceAttributes: map[string]string{
				"server.state": "standalone",
				"zk.version":   "3.4.14-4c25d480e66aadd371de8bd2fd8da255ac140bcf",
			},
			expectedLogs: []logMsg{
				{
					msg:   "metric computation failed",
					level: zapcore.DebugLevel,
				},
			},
			expectedNumResourceMetrics: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localAddr := testutil.GetAvailableLocalAddress(t)
			if !tt.mockZKConnectionErr {
				ms := mockedServer{ready: make(chan bool, 1)}
				go ms.mockZKServer(t, localAddr, tt.mockedZKOutputSourceFilename)
				<-ms.ready
			}

			cfg := createDefaultConfig().(*Config)
			cfg.TCPAddr.Endpoint = localAddr
			if tt.metricsSettings != nil {
				cfg.Metrics = tt.metricsSettings()
			}

			core, observedLogs := observer.New(zap.DebugLevel)
			z, err := newZookeeperMetricsScraper(zap.New(core), cfg)
			require.NoError(t, err)
			require.Equal(t, "zookeeper", z.Name())

			ctx := context.Background()

			if tt.setConnectionDeadline != nil {
				z.setConnectionDeadline = tt.setConnectionDeadline
			}

			if tt.closeConnection != nil {
				z.closeConnection = tt.closeConnection
			}

			if tt.sendCmd != nil {
				z.sendCmd = tt.sendCmd
			}

			actualMetrics, err := z.scrape(ctx)
			require.NoError(t, z.shutdown(ctx))

			require.Equal(t, len(tt.expectedLogs), observedLogs.Len())
			for i, log := range tt.expectedLogs {
				require.Equal(t, log.msg, observedLogs.All()[i].Message)
				require.Equal(t, log.level, observedLogs.All()[i].Level)
			}

			if tt.expectedNumResourceMetrics == 0 {
				if tt.wantErr {
					require.Error(t, err)
					require.Equal(t, pdata.NewMetrics(), actualMetrics)
				}

				require.NoError(t, z.shutdown(ctx))
				return
			}

			expectedFile := filepath.Join("testdata", "scraper", fmt.Sprintf("%s.json", tt.expectedMetricsFilename))
			expectedMetrics, err := golden.ReadMetrics(expectedFile)
			require.NoError(t, err)

			require.NoError(t, scrapertest.CompareMetrics(expectedMetrics, actualMetrics))
		})
	}
}

func TestZookeeperShutdownBeforeScrape(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	z, err := newZookeeperMetricsScraper(zap.NewNop(), cfg)
	require.NoError(t, err)
	require.NoError(t, z.shutdown(context.Background()))
}

type mockedServer struct {
	ready chan bool
}

func (ms *mockedServer) mockZKServer(t *testing.T, endpoint string, filename string) {
	listener, err := net.Listen("tcp", endpoint)
	require.NoError(t, err)
	defer listener.Close()

	ms.ready <- true

	conn, err := listener.Accept()
	require.NoError(t, err)

	for {
		out, err := ioutil.ReadFile(filepath.Join("testdata", filename))
		require.NoError(t, err)

		conn.Write(out)
		conn.Close()
		return
	}
}
