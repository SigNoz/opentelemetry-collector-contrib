// Copyright The OpenTelemetry Authors
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

package tanzuobservabilityexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/config/confighttp"
)

func TestConfigRequiresNonEmptyEndpoint(t *testing.T) {
	c := &Config{
		ExporterSettings: config.ExporterSettings{},
		Traces: TracesConfig{
			HTTPClientSettings: confighttp.HTTPClientSettings{Endpoint: ""},
		},
		Metrics: MetricsConfig{
			HTTPClientSettings: confighttp.HTTPClientSettings{Endpoint: "http://localhost:2878"},
		},
	}

	assert.Error(t, c.Validate())
}

func TestConfigRequiresValidEndpointUrl(t *testing.T) {
	c := &Config{
		ExporterSettings: config.ExporterSettings{},
		Traces: TracesConfig{
			HTTPClientSettings: confighttp.HTTPClientSettings{Endpoint: "http#$%^&#$%&#"},
		},
		Metrics: MetricsConfig{
			HTTPClientSettings: confighttp.HTTPClientSettings{Endpoint: "http://localhost:2878"},
		},
	}

	assert.Error(t, c.Validate())
}

func TestMetricsConfigRequiresNonEmptyEndpoint(t *testing.T) {
	c := &Config{
		ExporterSettings: config.ExporterSettings{},
		Traces: TracesConfig{
			HTTPClientSettings: confighttp.HTTPClientSettings{Endpoint: "http://localhost:30001"},
		},
		Metrics: MetricsConfig{
			HTTPClientSettings: confighttp.HTTPClientSettings{Endpoint: ""},
		},
	}

	assert.Error(t, c.Validate())
}

func TestMetricsConfigRequiresValidEndpointUrl(t *testing.T) {
	c := &Config{
		ExporterSettings: config.ExporterSettings{},
		Traces: TracesConfig{
			HTTPClientSettings: confighttp.HTTPClientSettings{Endpoint: "http://localhost:30001"},
		},
		Metrics: MetricsConfig{
			HTTPClientSettings: confighttp.HTTPClientSettings{Endpoint: "http#$%^&#$%&#"},
		},
	}

	assert.Error(t, c.Validate())
}

func TestDifferentHostNames(t *testing.T) {
	c := &Config{
		ExporterSettings: config.ExporterSettings{},
		Traces: TracesConfig{
			HTTPClientSettings: confighttp.HTTPClientSettings{Endpoint: "http://localhost:30001"},
		},
		Metrics: MetricsConfig{
			HTTPClientSettings: confighttp.HTTPClientSettings{Endpoint: "http://foo.com:2878"},
		},
	}
	assert.Error(t, c.Validate())
}

func TestConfigNormal(t *testing.T) {
	c := &Config{
		ExporterSettings: config.ExporterSettings{},
		Traces: TracesConfig{
			HTTPClientSettings: confighttp.HTTPClientSettings{Endpoint: "http://localhost:40001"},
		},
		Metrics: MetricsConfig{
			HTTPClientSettings: confighttp.HTTPClientSettings{Endpoint: "http://localhost:2916"},
		},
	}
	assert.NoError(t, c.Validate())
}
