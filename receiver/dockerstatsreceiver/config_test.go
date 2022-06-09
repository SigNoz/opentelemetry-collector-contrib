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

package dockerstatsreceiver

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/receiver/scraperhelper"
	"go.opentelemetry.io/collector/service/servicetest"
)

func TestLoadConfig(t *testing.T) {
	factories, err := componenttest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Receivers[typeStr] = factory
	cfg, err := servicetest.LoadConfigAndValidate(filepath.Join("testdata", "config.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, 2, len(cfg.Receivers))

	defaultConfig := cfg.Receivers[config.NewComponentID(typeStr)]
	assert.Equal(t, factory.CreateDefaultConfig(), defaultConfig)

	dcfg := defaultConfig.(*Config)
	assert.Equal(t, "docker_stats", dcfg.ID().String())
	assert.Equal(t, "unix:///var/run/docker.sock", dcfg.Endpoint)
	assert.Equal(t, 10*time.Second, dcfg.CollectionInterval)
	assert.Equal(t, 5*time.Second, dcfg.Timeout)
	assert.Equal(t, defaultDockerAPIVersion, dcfg.DockerAPIVersion)

	assert.Nil(t, dcfg.ExcludedImages)
	assert.Nil(t, dcfg.ContainerLabelsToMetricLabels)
	assert.Nil(t, dcfg.EnvVarsToMetricLabels)

	assert.False(t, dcfg.ProvidePerCoreCPUMetrics)

	ascfg := cfg.Receivers[config.NewComponentIDWithName(typeStr, "allsettings")].(*Config)
	assert.Equal(t, "docker_stats/allsettings", ascfg.ID().String())
	assert.Equal(t, "http://example.com/", ascfg.Endpoint)
	assert.Equal(t, 2*time.Second, ascfg.CollectionInterval)
	assert.Equal(t, 20*time.Second, ascfg.Timeout)
	assert.Equal(t, 1.24, ascfg.DockerAPIVersion)

	assert.Equal(t, []string{
		"undesired-container",
		"another-*-container",
	}, ascfg.ExcludedImages)

	assert.Equal(t, map[string]string{
		"my.container.label":       "my-metric-label",
		"my.other.container.label": "my-other-metric-label",
	}, ascfg.ContainerLabelsToMetricLabels)

	assert.Equal(t, map[string]string{
		"MY_ENVIRONMENT_VARIABLE":       "my-metric-label",
		"MY_OTHER_ENVIRONMENT_VARIABLE": "my-other-metric-label",
	}, ascfg.EnvVarsToMetricLabels)

	assert.True(t, ascfg.ProvidePerCoreCPUMetrics)
}

func TestValidateErrors(t *testing.T) {
	cfg := &Config{}
	assert.Equal(t, "endpoint must be specified", cfg.Validate().Error())

	cfg = &Config{Endpoint: "someEndpoint"}
	assert.Equal(t, "collection_interval must be a positive duration", cfg.Validate().Error())

	cfg = &Config{ScraperControllerSettings: scraperhelper.ScraperControllerSettings{CollectionInterval: 1 * time.Second}, Endpoint: "someEndpoint", DockerAPIVersion: 1.21}
	assert.Equal(t, "api_version must be at least 1.22", cfg.Validate().Error())
}
