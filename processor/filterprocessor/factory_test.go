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

package filterprocessor

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/config/configtest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/service/servicetest"
)

func TestType(t *testing.T) {
	factory := NewFactory()
	pType := factory.Type()

	assert.Equal(t, pType, config.Type("filter"))
}

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.Equal(t, cfg, &Config{
		ProcessorSettings: config.NewProcessorSettings(config.NewComponentID(typeStr)),
	})
	assert.NoError(t, configtest.CheckConfigStruct(cfg))
}

func TestCreateProcessors(t *testing.T) {
	tests := []struct {
		configName string
		succeed    bool
	}{
		{
			configName: "config_regexp.yaml",
			succeed:    true,
		}, {
			configName: "config_strict.yaml",
			succeed:    true,
		}, {
			configName: "config_invalid.yaml",
			succeed:    false,
		}, {
			configName: "config_logs_strict.yaml",
			succeed:    true,
		}, {
			configName: "config_logs_regexp.yaml",
			succeed:    true,
		}, {
			configName: "config_logs_record_attributes_strict.yaml",
			succeed:    true,
		}, {
			configName: "config_logs_record_attributes_regexp.yaml",
			succeed:    true,
		},
	}

	for _, test := range tests {
		factories, err := componenttest.NopFactories()
		assert.Nil(t, err)

		factory := NewFactory()
		factories.Processors[typeStr] = factory
		cfg, err := servicetest.LoadConfigAndValidate(filepath.Join("testdata", test.configName), factories)
		assert.Nil(t, err)

		for name, cfg := range cfg.Processors {
			t.Run(fmt.Sprintf("%s/%s", test.configName, name), func(t *testing.T) {
				factory := NewFactory()

				tp, tErr := factory.CreateTracesProcessor(context.Background(), componenttest.NewNopProcessorCreateSettings(), cfg, consumertest.NewNop())
				// Not implemented error
				assert.NotNil(t, tErr)
				assert.Nil(t, tp)

				mp, mErr := factory.CreateMetricsProcessor(context.Background(), componenttest.NewNopProcessorCreateSettings(), cfg, consumertest.NewNop())
				assert.Equal(t, test.succeed, mp != nil)
				assert.Equal(t, test.succeed, mErr == nil)
			})
		}
	}
}
