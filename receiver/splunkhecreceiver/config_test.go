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

package splunkhecreceiver

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/service/servicetest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/splunk"
)

func TestLoadConfig(t *testing.T) {
	factories, err := componenttest.NopFactories()
	assert.Nil(t, err)

	factory := NewFactory()
	factories.Receivers[typeStr] = factory
	cfg, err := servicetest.LoadConfigAndValidate(filepath.Join("testdata", "config.yaml"), factories)

	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, len(cfg.Receivers), 3)

	r0 := cfg.Receivers[config.NewComponentID(typeStr)].(*Config)
	assert.Equal(t, r0, createDefaultConfig())

	r1 := cfg.Receivers[config.NewComponentIDWithName(typeStr, "allsettings")].(*Config)
	expectedAllSettings := &Config{
		ReceiverSettings: config.NewReceiverSettings(config.NewComponentIDWithName(typeStr, "allsettings")),
		HTTPServerSettings: confighttp.HTTPServerSettings{
			Endpoint: "localhost:8088",
		},
		AccessTokenPassthroughConfig: splunk.AccessTokenPassthroughConfig{
			AccessTokenPassthrough: true,
		},
		RawPath: "/foo",
		HecToOtelAttrs: splunk.HecToOtelAttrs{
			Source:     "file.name",
			SourceType: "foobar",
			Index:      "myindex",
			Host:       "myhostfield",
		},
	}
	assert.Equal(t, expectedAllSettings, r1)

	r2 := cfg.Receivers[config.NewComponentIDWithName(typeStr, "tls")].(*Config)
	expectedTLSConfig := &Config{
		ReceiverSettings: config.NewReceiverSettings(config.NewComponentIDWithName(typeStr, "tls")),
		HTTPServerSettings: confighttp.HTTPServerSettings{
			Endpoint: ":8088",
			TLSSetting: &configtls.TLSServerSetting{
				TLSSetting: configtls.TLSSetting{
					CertFile: "/test.crt",
					KeyFile:  "/test.key",
				},
			},
		},
		AccessTokenPassthroughConfig: splunk.AccessTokenPassthroughConfig{
			AccessTokenPassthrough: false,
		},
		RawPath: "/services/collector/raw",
		HecToOtelAttrs: splunk.HecToOtelAttrs{
			Source:     "com.splunk.source",
			SourceType: "com.splunk.sourcetype",
			Index:      "com.splunk.index",
			Host:       "host.name",
		},
	}
	assert.Equal(t, expectedTLSConfig, r2)
}
