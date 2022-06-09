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

package carbonreceiver

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenterror"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/testutil"
	internaldata "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/opencensus"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/carbonreceiver/protocol"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/carbonreceiver/transport"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/carbonreceiver/transport/client"
)

func Test_carbonreceiver_New(t *testing.T) {
	defaultConfig := createDefaultConfig().(*Config)
	type args struct {
		config       Config
		nextConsumer consumer.Metrics
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "default_config",
			args: args{
				config:       *defaultConfig,
				nextConsumer: consumertest.NewNop(),
			},
		},
		{
			name: "zero_value_parser",
			args: args{
				config: Config{
					ReceiverSettings: defaultConfig.ReceiverSettings,
					NetAddr: confignet.NetAddr{
						Endpoint:  defaultConfig.Endpoint,
						Transport: defaultConfig.Transport,
					},
					TCPIdleTimeout: defaultConfig.TCPIdleTimeout,
				},
				nextConsumer: consumertest.NewNop(),
			},
		},
		{
			name: "nil_nextConsumer",
			args: args{
				config: *defaultConfig,
			},
			wantErr: componenterror.ErrNilNextConsumer,
		},
		{
			name: "empty_endpoint",
			args: args{
				config: Config{
					ReceiverSettings: config.NewReceiverSettings(config.NewComponentID(typeStr)),
				},
				nextConsumer: consumertest.NewNop(),
			},
			wantErr: errEmptyEndpoint,
		},
		{
			name: "invalid_transport",
			args: args{
				config: Config{
					ReceiverSettings: config.NewReceiverSettings(config.NewComponentIDWithName(typeStr, "invalid_transport_rcv")),
					NetAddr: confignet.NetAddr{
						Endpoint:  "localhost:2003",
						Transport: "unknown_transp",
					},
					Parser: &protocol.Config{
						Type:   "plaintext",
						Config: &protocol.PlaintextConfig{},
					},
				},
				nextConsumer: consumertest.NewNop(),
			},
			wantErr: errors.New("unsupported transport \"unknown_transp\" for receiver carbon/invalid_transport_rcv"),
		},
		{
			name: "regex_parser",
			args: args{
				config: Config{
					ReceiverSettings: config.NewReceiverSettings(config.NewComponentID(typeStr)),
					NetAddr: confignet.NetAddr{
						Endpoint:  "localhost:2003",
						Transport: "tcp",
					},
					Parser: &protocol.Config{
						Type: "regex",
						Config: &protocol.RegexParserConfig{
							Rules: []*protocol.RegexRule{
								{
									Regexp: `(?P<key_root>[^.]*)\.test`,
								},
							},
						},
					},
				},
				nextConsumer: consumertest.NewNop(),
			},
		},
		{
			name: "negative_tcp_idle_timeout",
			args: args{
				config: Config{
					ReceiverSettings: config.NewReceiverSettings(config.NewComponentID(typeStr)),
					NetAddr: confignet.NetAddr{
						Endpoint:  "localhost:2003",
						Transport: "tcp",
					},
					TCPIdleTimeout: -1 * time.Second,
					Parser: &protocol.Config{
						Type:   "plaintext",
						Config: &protocol.PlaintextConfig{},
					},
				},
				nextConsumer: consumertest.NewNop(),
			},
			wantErr: errors.New("invalid idle timeout: -1s"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(componenttest.NewNopReceiverCreateSettings(), tt.args.config, tt.args.nextConsumer)
			assert.Equal(t, tt.wantErr, err)
			if err == nil {
				require.NotNil(t, got)
				assert.NoError(t, got.Shutdown(context.Background()))
			} else {
				assert.Nil(t, got)
			}
		})
	}
}

func Test_carbonreceiver_EndToEnd(t *testing.T) {
	host := "localhost"
	port := int(testutil.GetAvailablePort(t))
	tests := []struct {
		name     string
		configFn func() *Config
		clientFn func(t *testing.T) *client.Graphite
	}{
		{
			name: "default_config",
			configFn: func() *Config {
				return createDefaultConfig().(*Config)
			},
			clientFn: func(t *testing.T) *client.Graphite {
				c, err := client.NewGraphite(client.TCP, host, port)
				require.NoError(t, err)
				return c
			},
		},
		{
			name: "default_config_udp",
			configFn: func() *Config {
				cfg := createDefaultConfig().(*Config)
				cfg.Transport = "udp"
				return cfg
			},
			clientFn: func(t *testing.T) *client.Graphite {
				c, err := client.NewGraphite(client.UDP, host, port)
				require.NoError(t, err)
				return c
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.configFn()
			cfg.Endpoint = fmt.Sprintf("%s:%d", host, port)
			sink := new(consumertest.MetricsSink)
			rcv, err := New(componenttest.NewNopReceiverCreateSettings(), *cfg, sink)
			require.NoError(t, err)
			r := rcv.(*carbonReceiver)

			mr := transport.NewMockReporter(1)
			r.reporter = mr

			require.NoError(t, r.Start(context.Background(), componenttest.NewNopHost()))
			runtime.Gosched()
			defer r.Shutdown(context.Background())

			snd := tt.clientFn(t)

			ts := time.Now()
			carbonMetric := client.Metric{
				Name:      "tst_dbl",
				Value:     1.23,
				Timestamp: ts,
			}
			err = snd.SendMetric(carbonMetric)
			require.NoError(t, err)

			mr.WaitAllOnMetricsProcessedCalls()

			mdd := sink.AllMetrics()
			require.Len(t, mdd, 1)
			_, _, metrics := internaldata.ResourceMetricsToOC(mdd[0].ResourceMetrics().At(0))
			require.Len(t, metrics, 1)
			assert.Equal(t, carbonMetric.Name, metrics[0].GetMetricDescriptor().GetName())
			tss := metrics[0].GetTimeseries()
			require.Equal(t, 1, len(tss))
		})
	}
}
