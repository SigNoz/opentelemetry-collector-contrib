// Copyright  The OpenTelemetry Authors
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

package awsfirehosereceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configtest"
	"go.opentelemetry.io/collector/consumer/consumertest"
)

func TestValidConfig(t *testing.T) {
	err := configtest.CheckConfigStruct(createDefaultConfig())
	require.NoError(t, err)
}

func TestCreateMetricsReceiver(t *testing.T) {
	r, err := createMetricsReceiver(
		context.Background(),
		componenttest.NewNopReceiverCreateSettings(),
		createDefaultConfig(),
		consumertest.NewNop(),
	)
	require.NoError(t, err)
	require.NotNil(t, r)
}

func TestValidateRecordType(t *testing.T) {
	require.NoError(t, validateRecordType(defaultRecordType))
	require.Error(t, validateRecordType("nop"))
}
