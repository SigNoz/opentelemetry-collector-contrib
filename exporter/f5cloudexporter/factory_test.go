// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package f5cloudexporter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/config/configtest"
	"golang.org/x/oauth2"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/testutil"
)

func TestFactory_TestType(t *testing.T) {
	f := NewFactory()
	assert.Equal(t, f.Type(), config.Type(typeStr))
}

func TestFactory_CreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	assert.NoError(t, configtest.CheckConfigStruct(cfg))
	ocfg, ok := factory.CreateDefaultConfig().(*Config)
	assert.True(t, ok)
	assert.Equal(t, ocfg.HTTPClientSettings.Endpoint, "")
	assert.Equal(t, ocfg.HTTPClientSettings.Timeout, 30*time.Second, "default timeout is 30 seconds")
	assert.Equal(t, ocfg.RetrySettings.Enabled, true, "default retry is enabled")
	assert.Equal(t, ocfg.RetrySettings.MaxElapsedTime, 300*time.Second, "default retry MaxElapsedTime")
	assert.Equal(t, ocfg.RetrySettings.InitialInterval, 5*time.Second, "default retry InitialInterval")
	assert.Equal(t, ocfg.RetrySettings.MaxInterval, 30*time.Second, "default retry MaxInterval")
	assert.Equal(t, ocfg.QueueSettings.Enabled, true, "default sending queue is enabled")
}

func TestFactory_CreateMetricsExporter(t *testing.T) {
	factory := NewFactoryWithTokenSourceGetter(mockTokenSourceGetter)
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.HTTPClientSettings.Endpoint = "https://" + testutil.GetAvailableLocalAddress(t)
	cfg.Source = "tests"
	cfg.AuthConfig = AuthConfig{
		CredentialFile: "testdata/empty_credential_file.json",
		Audience:       "tests",
	}

	creationParams := componenttest.NewNopExporterCreateSettings()
	creationParams.BuildInfo = component.BuildInfo{
		Version: "0.0.0",
	}
	oexp, err := factory.CreateMetricsExporter(context.Background(), creationParams, cfg)
	require.Nil(t, err)
	require.NotNil(t, oexp)

	require.Equal(t, "opentelemetry-collector-contrib 0.0.0", cfg.Headers["User-Agent"])
}

func TestFactory_CreateMetricsExporterInvalidConfig(t *testing.T) {
	factory := NewFactoryWithTokenSourceGetter(mockTokenSourceGetter)
	cfg := factory.CreateDefaultConfig().(*Config)

	creationParams := componenttest.NewNopExporterCreateSettings()
	oexp, err := factory.CreateMetricsExporter(context.Background(), creationParams, cfg)
	require.Error(t, err)
	require.Nil(t, oexp)
}

func TestFactory_CreateTracesExporter(t *testing.T) {
	factory := NewFactoryWithTokenSourceGetter(mockTokenSourceGetter)
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.HTTPClientSettings.Endpoint = "https://" + testutil.GetAvailableLocalAddress(t)
	cfg.Source = "tests"
	cfg.AuthConfig = AuthConfig{
		CredentialFile: "testdata/empty_credential_file.json",
		Audience:       "tests",
	}

	creationParams := componenttest.NewNopExporterCreateSettings()
	creationParams.BuildInfo = component.BuildInfo{
		Version: "0.0.0",
	}
	oexp, err := factory.CreateTracesExporter(context.Background(), creationParams, cfg)
	require.Nil(t, err)
	require.NotNil(t, oexp)

	require.Equal(t, "opentelemetry-collector-contrib 0.0.0", cfg.Headers["User-Agent"])
}

func Test_Factory_CreateTracesExporterInvalidConfig(t *testing.T) {
	factory := NewFactoryWithTokenSourceGetter(mockTokenSourceGetter)
	cfg := factory.CreateDefaultConfig().(*Config)

	creationParams := componenttest.NewNopExporterCreateSettings()
	oexp, err := factory.CreateTracesExporter(context.Background(), creationParams, cfg)
	require.Error(t, err)
	require.Nil(t, oexp)
}

func TestFactory_CreateLogsExporter(t *testing.T) {
	factory := NewFactoryWithTokenSourceGetter(mockTokenSourceGetter)
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.HTTPClientSettings.Endpoint = "https://" + testutil.GetAvailableLocalAddress(t)
	cfg.Source = "tests"
	cfg.AuthConfig = AuthConfig{
		CredentialFile: "testdata/empty_credential_file.json",
		Audience:       "tests",
	}

	creationParams := componenttest.NewNopExporterCreateSettings()
	creationParams.BuildInfo = component.BuildInfo{
		Version: "0.0.0",
	}
	oexp, err := factory.CreateLogsExporter(context.Background(), creationParams, cfg)
	require.Nil(t, err)
	require.NotNil(t, oexp)

	require.Equal(t, "opentelemetry-collector-contrib 0.0.0", cfg.Headers["User-Agent"])
}

func TestFactory_CreateLogsExporterInvalidConfig(t *testing.T) {
	factory := NewFactoryWithTokenSourceGetter(mockTokenSourceGetter)
	cfg := factory.CreateDefaultConfig().(*Config)

	creationParams := componenttest.NewNopExporterCreateSettings()
	oexp, err := factory.CreateLogsExporter(context.Background(), creationParams, cfg)
	require.Error(t, err)
	require.Nil(t, oexp)
}

func TestFactory_getTokenSourceFromConfig(t *testing.T) {
	factory := NewFactoryWithTokenSourceGetter(mockTokenSourceGetter)
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.HTTPClientSettings.Endpoint = "https://" + testutil.GetAvailableLocalAddress(t)
	cfg.Source = "tests"
	cfg.AuthConfig = AuthConfig{
		CredentialFile: "testdata/empty_credential_file.json",
		Audience:       "tests",
	}

	ts, err := getTokenSourceFromConfig(cfg)
	assert.Error(t, err)
	assert.Nil(t, ts)
}

func mockTokenSourceGetter(_ *Config) (oauth2.TokenSource, error) {
	tkn := &oauth2.Token{
		AccessToken:  "",
		TokenType:    "",
		RefreshToken: "",
		Expiry:       time.Time{},
	}

	return oauth2.StaticTokenSource(tkn), nil
}
