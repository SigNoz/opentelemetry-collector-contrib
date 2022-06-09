// Copyright 2019 OpenTelemetry Authors
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

package honeycombexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/honeycombexporter"

import (
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type Config struct {
	config.ExporterSettings `mapstructure:",squash"`
	// APIKey is the authentication token associated with the Honeycomb account.
	APIKey string `mapstructure:"api_key"`
	// Dataset is the Honeycomb dataset to send events to.
	Dataset string `mapstructure:"dataset"`
	// API URL to use (defaults to https://api.honeycomb.io)
	APIURL string `mapstructure:"api_url"`
	// Deprecated - do not use. This will be removed in a future release.
	SampleRate uint `mapstructure:"sample_rate"`
	// The name of an attribute that contains the sample_rate for each span.
	// If the attribute is on the span, it takes precedence over the static sample_rate configuration
	SampleRateAttribute string `mapstructure:"sample_rate_attribute"`
	// Debug enables more verbose logging from the Honeycomb SDK. It defaults to false.
	Debug bool `mapstructure:"debug"`
	// RetrySettings helps configure retry on traces which failed to send
	exporterhelper.RetrySettings `mapstructure:"retry_on_failure"`
	// QueueSettings enable queued processing
	exporterhelper.QueueSettings `mapstructure:"sending_queue"`
}

func (cfg *Config) Validate() error {
	return nil
}
