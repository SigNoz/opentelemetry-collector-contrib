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

package spanprocessor

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/service/servicetest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/processor/filterconfig"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/processor/filterset"
)

func TestLoadConfig(t *testing.T) {
	factories, err := componenttest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Processors[typeStr] = factory

	cfg, err := servicetest.LoadConfigAndValidate(filepath.Join("testdata", "config.yaml"), factories)

	assert.NoError(t, err)
	assert.NotNil(t, cfg)

	p0 := cfg.Processors[config.NewComponentIDWithName("span", "custom")]
	assert.Equal(t, p0, &Config{
		ProcessorSettings: config.NewProcessorSettings(config.NewComponentIDWithName("span", "custom")),
		Rename: Name{
			FromAttributes: []string{"db.svc", "operation", "id"},
			Separator:      "::",
		},
	})

	p1 := cfg.Processors[config.NewComponentIDWithName("span", "no-separator")]
	assert.Equal(t, p1, &Config{
		ProcessorSettings: config.NewProcessorSettings(config.NewComponentIDWithName("span", "no-separator")),
		Rename: Name{
			FromAttributes: []string{"db.svc", "operation", "id"},
			Separator:      "",
		},
	})

	p2 := cfg.Processors[config.NewComponentIDWithName("span", "to_attributes")]
	assert.Equal(t, p2, &Config{
		ProcessorSettings: config.NewProcessorSettings(config.NewComponentIDWithName("span", "to_attributes")),
		Rename: Name{
			ToAttributes: &ToAttributes{
				Rules: []string{`^\/api\/v1\/document\/(?P<documentId>.*)\/update$`},
			},
		},
	})

	p3 := cfg.Processors[config.NewComponentIDWithName("span", "includeexclude")]
	assert.Equal(t, p3, &Config{
		ProcessorSettings: config.NewProcessorSettings(config.NewComponentIDWithName("span", "includeexclude")),
		MatchConfig: filterconfig.MatchConfig{
			Include: &filterconfig.MatchProperties{
				Config:    *createMatchConfig(filterset.Regexp),
				Services:  []string{`banks`},
				SpanNames: []string{"^(.*?)/(.*?)$"},
			},
			Exclude: &filterconfig.MatchProperties{
				Config:    *createMatchConfig(filterset.Strict),
				SpanNames: []string{`donot/change`},
			},
		},
		Rename: Name{
			ToAttributes: &ToAttributes{
				Rules: []string{`(?P<operation_website>.*?)$`},
			},
		},
	})

	// Set name
	p4 := cfg.Processors[config.NewComponentIDWithName("span", "set_status_err")]
	assert.Equal(t, p4, &Config{
		ProcessorSettings: config.NewProcessorSettings(config.NewComponentIDWithName("span", "set_status_err")),
		SetStatus: &Status{
			Code:        "Error",
			Description: "some additional error description",
		},
	})

	p5 := cfg.Processors[config.NewComponentIDWithName("span", "set_status_ok")]
	assert.Equal(t, p5, &Config{
		ProcessorSettings: config.NewProcessorSettings(config.NewComponentIDWithName("span", "set_status_ok")),
		MatchConfig: filterconfig.MatchConfig{
			Include: &filterconfig.MatchProperties{
				Attributes: []filterconfig.Attribute{
					{Key: "http.status_code", Value: 400},
				},
			},
		},
		SetStatus: &Status{
			Code: "Ok",
		},
	})
}

func createMatchConfig(matchType filterset.MatchType) *filterset.Config {
	return &filterset.Config{
		MatchType: matchType,
	}
}
