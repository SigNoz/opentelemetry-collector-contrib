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

package awskinesisexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awskinesisexporter"

import (
	"fmt"

	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// AWSConfig contains AWS specific configuration such as awskinesis stream, region, etc.
type AWSConfig struct {
	StreamName      string `mapstructure:"stream_name"`
	KinesisEndpoint string `mapstructure:"kinesis_endpoint"`
	Region          string `mapstructure:"region"`
	Role            string `mapstructure:"role"`
}

type Encoding struct {
	Name        string `mapstructure:"name"`
	Compression string `mapstructure:"compression"`
}

// Config contains the main configuration options for the awskinesis exporter
type Config struct {
	config.ExporterSettings        `mapstructure:",squash"`
	exporterhelper.TimeoutSettings `mapstructure:",squash"`
	exporterhelper.QueueSettings   `mapstructure:"sending_queue"`
	exporterhelper.RetrySettings   `mapstructure:"retry_on_failure"`

	Encoding           `mapstructure:"encoding"`
	AWS                AWSConfig `mapstructure:"aws"`
	MaxRecordsPerBatch int       `mapstructure:"max_records_per_batch"`
	MaxRecordSize      int       `mapstructure:"max_record_size"`
}

// Validate checks if the exporter configuration is valid
func (cfg *Config) Validate() error {
	if err := cfg.QueueSettings.Validate(); err != nil {
		return fmt.Errorf("queue settings has invalid configuration: %w", err)
	}

	return nil
}

var _ config.Exporter = (*Config)(nil)
