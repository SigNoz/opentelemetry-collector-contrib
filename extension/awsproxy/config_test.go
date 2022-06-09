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

package awsproxy

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/service/servicetest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/proxy"
)

func TestLoadConfig(t *testing.T) {
	factories, err := componenttest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Extensions[typeStr] = factory
	cfg, err := servicetest.LoadConfigAndValidate(filepath.Join("testdata", "config.yaml"), factories)

	require.Nil(t, err)
	require.NotNil(t, cfg)

	ext0 := cfg.Extensions[config.NewComponentID(typeStr)]
	assert.Equal(t, factory.CreateDefaultConfig(), ext0)

	ext1 := cfg.Extensions[config.NewComponentIDWithName(typeStr, "1")]
	assert.Equal(t,
		&Config{
			ExtensionSettings: config.NewExtensionSettings(config.NewComponentIDWithName(typeStr, "1")),
			ProxyConfig: proxy.Config{
				TCPAddr: confignet.TCPAddr{
					Endpoint: "0.0.0.0:1234",
				},
				ProxyAddress: "https://proxy.proxy.com",
				TLSSetting: configtls.TLSClientSetting{
					Insecure:   true,
					ServerName: "something",
				},
				Region:      "us-west-1",
				RoleARN:     "arn:aws:iam::123456789012:role/awesome_role",
				AWSEndpoint: "https://another.aws.endpoint.com",
			},
		},
		ext1)

	assert.Equal(t, 1, len(cfg.Service.Extensions))
	assert.Equal(t, config.NewComponentIDWithName(typeStr, "1"), cfg.Service.Extensions[0])
}
