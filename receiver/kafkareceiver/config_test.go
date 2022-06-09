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

package kafkareceiver

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/service/servicetest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter"
)

func TestLoadConfig(t *testing.T) {
	factories, err := componenttest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Receivers[typeStr] = factory
	cfg, err := servicetest.LoadConfigAndValidate(filepath.Join("testdata", "config.yaml"), factories)
	require.NoError(t, err)
	require.Equal(t, 1, len(cfg.Receivers))

	r := cfg.Receivers[config.NewComponentID(typeStr)].(*Config)
	assert.Equal(t, &Config{
		ReceiverSettings: config.NewReceiverSettings(config.NewComponentID(typeStr)),
		Topic:            "spans",
		Encoding:         "otlp_proto",
		Brokers:          []string{"foo:123", "bar:456"},
		ClientID:         "otel-collector",
		GroupID:          "otel-collector",
		Authentication: kafkaexporter.Authentication{
			TLS: &configtls.TLSClientSetting{
				TLSSetting: configtls.TLSSetting{
					CAFile:   "ca.pem",
					CertFile: "cert.pem",
					KeyFile:  "key.pem",
				},
			},
		},
		Metadata: kafkaexporter.Metadata{
			Full: true,
			Retry: kafkaexporter.MetadataRetry{
				Max:     10,
				Backoff: time.Second * 5,
			},
		},
		AutoCommit: AutoCommit{
			Enable:   true,
			Interval: 1 * time.Second,
		},
	}, r)
}
