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

package routingprocessor

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/loggingexporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/service/servicetest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/jaegerexporter"
)

func TestLoadConfig(t *testing.T) {
	testcases := []struct {
		configPath     string
		factoriesFunc  func(component.Factories) component.Factories
		expectedConfig *Config
	}{
		{
			configPath: "config_traces.yaml",
			factoriesFunc: func(factories component.Factories) component.Factories {
				// we don't need to use them in this test, but the config has them
				factories.Exporters["otlp"] = otlpexporter.NewFactory()
				factories.Exporters["jaeger"] = jaegerexporter.NewFactory()
				return factories
			},
			expectedConfig: &Config{
				ProcessorSettings: config.NewProcessorSettings(config.NewComponentID(typeStr)),
				DefaultExporters:  []string{"otlp"},
				AttributeSource:   "context",
				FromAttribute:     "X-Tenant",
				Table: []RoutingTableItem{
					{
						Value:     "acme",
						Exporters: []string{"jaeger/acme", "otlp/acme"},
					},
					{
						Value:     "globex",
						Exporters: []string{"otlp/globex"},
					},
				},
			},
		},
		{
			configPath: "config_metrics.yaml",
			factoriesFunc: func(factories component.Factories) component.Factories {
				// we don't need to use it in this test, but the config has them
				factories.Exporters["logging"] = loggingexporter.NewFactory()
				return factories
			},
			expectedConfig: &Config{
				ProcessorSettings: config.NewProcessorSettings(config.NewComponentID(typeStr)),
				DefaultExporters:  []string{"logging/default"},
				AttributeSource:   "context",
				FromAttribute:     "X-Custom-Metrics-Header",
				Table: []RoutingTableItem{
					{
						Value:     "acme",
						Exporters: []string{"logging/acme"},
					},
					{
						Value:     "globex",
						Exporters: []string{"logging/globex"},
					},
				},
			},
		},
		{
			configPath: "config_logs.yaml",
			factoriesFunc: func(factories component.Factories) component.Factories {
				// we don't need to use it in this test, but the config has them
				factories.Exporters["logging"] = loggingexporter.NewFactory()
				return factories
			},
			expectedConfig: &Config{
				ProcessorSettings: config.NewProcessorSettings(config.NewComponentID(typeStr)),
				DefaultExporters:  []string{"logging/default"},
				AttributeSource:   "context",
				FromAttribute:     "X-Custom-Logs-Header",
				Table: []RoutingTableItem{
					{
						Value:     "acme",
						Exporters: []string{"logging/acme"},
					},
					{
						Value:     "globex",
						Exporters: []string{"logging/globex"},
					},
				},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.configPath, func(t *testing.T) {
			tc := tc

			factories, err := componenttest.NopFactories()
			assert.NoError(t, err)
			factories.Processors[typeStr] = NewFactory()
			factories = tc.factoriesFunc(factories)

			cfg, err := servicetest.LoadConfigAndValidate(filepath.Join("testdata", tc.configPath), factories)
			require.NoError(t, err)
			require.NotNil(t, cfg)

			parsed := cfg.Processors[config.NewComponentID(typeStr)]
			assert.Equal(t, tc.expectedConfig, parsed)
		})
	}
}
