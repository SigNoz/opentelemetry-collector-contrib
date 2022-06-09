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

package system

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	conventions "go.opentelemetry.io/collector/model/semconv/v1.5.0"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourcedetectionprocessor/internal"
)

type mockMetadata struct {
	mock.Mock
}

func (m *mockMetadata) Hostname() (string, error) {
	args := m.MethodCalled("Hostname")
	return args.String(0), args.Error(1)
}

func (m *mockMetadata) FQDN() (string, error) {
	args := m.MethodCalled("FQDN")
	return args.String(0), args.Error(1)
}

func (m *mockMetadata) OSType() (string, error) {
	args := m.MethodCalled("OSType")
	return args.String(0), args.Error(1)
}

func TestNewDetector(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "Success Case Valid Config 'HostnameSources' set to 'os'",
			cfg: Config{
				HostnameSources: []string{"os"},
			},
		},
		{
			name: "Success Case Valid Config 'HostnameSources' set to 'dns'",
			cfg: Config{
				HostnameSources: []string{"dns"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detector, err := NewDetector(componenttest.NewNopProcessorCreateSettings(), tt.cfg)
			assert.NotNil(t, detector)
			assert.NoError(t, err)
		})
	}
}

func TestDetectFQDNAvailable(t *testing.T) {
	md := &mockMetadata{}
	md.On("FQDN").Return("fqdn", nil)
	md.On("OSType").Return("darwin", nil)

	detector := &Detector{provider: md, logger: zap.NewNop(), hostnameSources: []string{"dns"}}
	res, schemaURL, err := detector.Detect(context.Background())
	require.NoError(t, err)
	assert.Equal(t, conventions.SchemaURL, schemaURL)
	md.AssertExpectations(t)
	res.Attributes().Sort()

	expected := internal.NewResource(map[string]interface{}{
		conventions.AttributeHostName: "fqdn",
		conventions.AttributeOSType:   "darwin",
	})
	expected.Attributes().Sort()

	assert.Equal(t, expected, res)

}

func TestFallbackHostname(t *testing.T) {
	mdHostname := &mockMetadata{}
	mdHostname.On("Hostname").Return("hostname", nil)
	mdHostname.On("FQDN").Return("", errors.New("err"))
	mdHostname.On("OSType").Return("darwin", nil)

	detector := &Detector{provider: mdHostname, logger: zap.NewNop(), hostnameSources: []string{"dns", "os"}}
	res, schemaURL, err := detector.Detect(context.Background())
	require.NoError(t, err)
	assert.Equal(t, conventions.SchemaURL, schemaURL)
	mdHostname.AssertExpectations(t)
	res.Attributes().Sort()

	expected := internal.NewResource(map[string]interface{}{
		conventions.AttributeHostName: "hostname",
		conventions.AttributeOSType:   "darwin",
	})
	expected.Attributes().Sort()

	assert.Equal(t, expected, res)
}

func TestUseHostname(t *testing.T) {
	mdHostname := &mockMetadata{}
	mdHostname.On("Hostname").Return("hostname", nil)
	mdHostname.On("OSType").Return("darwin", nil)

	detector := &Detector{provider: mdHostname, logger: zap.NewNop(), hostnameSources: []string{"os"}}
	res, schemaURL, err := detector.Detect(context.Background())
	require.NoError(t, err)
	assert.Equal(t, conventions.SchemaURL, schemaURL)
	mdHostname.AssertExpectations(t)
	res.Attributes().Sort()

	expected := internal.NewResource(map[string]interface{}{
		conventions.AttributeHostName: "hostname",
		conventions.AttributeOSType:   "darwin",
	})
	expected.Attributes().Sort()

	assert.Equal(t, expected, res)
}

func TestDetectError(t *testing.T) {
	// FQDN and hostname fail with 'hostnameSources' set to 'dns'
	mdFQDN := &mockMetadata{}
	mdFQDN.On("OSType").Return("windows", nil)
	mdFQDN.On("FQDN").Return("", errors.New("err"))
	mdFQDN.On("Hostname").Return("", errors.New("err"))

	detector := &Detector{provider: mdFQDN, logger: zap.NewNop(), hostnameSources: []string{"dns"}}
	res, schemaURL, err := detector.Detect(context.Background())
	assert.Error(t, err)
	assert.Equal(t, "", schemaURL)
	assert.True(t, internal.IsEmptyResource(res))

	// hostname fail with 'hostnameSources' set to 'os'
	mdHostname := &mockMetadata{}
	mdHostname.On("OSType").Return("windows", nil)
	mdHostname.On("Hostname").Return("", errors.New("err"))

	detector = &Detector{provider: mdHostname, logger: zap.NewNop(), hostnameSources: []string{"os"}}
	res, schemaURL, err = detector.Detect(context.Background())
	assert.Error(t, err)
	assert.Equal(t, "", schemaURL)
	assert.True(t, internal.IsEmptyResource(res))

	// OS type fails
	mdOSType := &mockMetadata{}
	mdOSType.On("FQDN").Return("fqdn", nil)
	mdOSType.On("OSType").Return("", errors.New("err"))

	detector = &Detector{provider: mdOSType, logger: zap.NewNop(), hostnameSources: []string{"dns"}}
	res, schemaURL, err = detector.Detect(context.Background())
	assert.Error(t, err)
	assert.Equal(t, "", schemaURL)
	assert.True(t, internal.IsEmptyResource(res))
}
