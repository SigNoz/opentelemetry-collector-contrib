// Copyright 2019, OpenTelemetry Authors
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

package signalfxexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/signalfxexporter"

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/signalfxexporter/internal/correlation"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/signalfxexporter/internal/translation"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/signalfxexporter/internal/translation/dpfilters"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/splunk"
)

const (
	translationRulesConfigKey = "translation_rules"
)

var _ config.Unmarshallable = (*Config)(nil)

// Config defines configuration for SignalFx exporter.
type Config struct {
	config.ExporterSettings        `mapstructure:",squash"`
	exporterhelper.TimeoutSettings `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	exporterhelper.QueueSettings   `mapstructure:"sending_queue"`
	exporterhelper.RetrySettings   `mapstructure:"retry_on_failure"`

	// AccessToken is the authentication token provided by SignalFx.
	AccessToken string `mapstructure:"access_token"`

	// Realm is the SignalFx realm where data is going to be sent to.
	Realm string `mapstructure:"realm"`

	// IngestURL is the destination to where SignalFx metrics will be sent to, it is
	// intended for tests and debugging. The value of Realm is ignored if the
	// URL is specified. The exporter will automatically append the appropriate
	// path: "/v2/datapoint" for metrics, and "/v2/event" for events.
	IngestURL string `mapstructure:"ingest_url"`

	// APIURL is the destination to where SignalFx metadata will be sent. This
	// value takes precedence over the value of Realm
	APIURL string `mapstructure:"api_url"`

	// Headers are a set of headers to be added to the HTTP request sending
	// trace data. These can override pre-defined header values used by the
	// exporter, eg: "User-Agent" can be set to a custom value if specified
	// here.
	Headers map[string]string `mapstructure:"headers"`

	// Whether to log datapoints dispatched to Splunk Observability Cloud
	LogDataPoints bool `mapstructure:"log_data_points"`

	// Whether to log dimension updates being sent to SignalFx.
	LogDimensionUpdates bool `mapstructure:"log_dimension_updates"`

	splunk.AccessTokenPassthroughConfig `mapstructure:",squash"`

	// TranslationRules defines a set of rules how to translate metrics to a SignalFx compatible format
	// Rules defined in translation/constants.go are used by default.
	TranslationRules []translation.Rule `mapstructure:"translation_rules"`

	// DeltaTranslationTTL specifies in seconds the max duration to keep the most recent datapoint for any
	// `delta_metric` specified in TranslationRules. Default is 3600s.
	DeltaTranslationTTL int64 `mapstructure:"delta_translation_ttl"`

	// SyncHostMetadata defines if the exporter should scrape host metadata and
	// sends it as property updates to SignalFx backend.
	// IMPORTANT: Host metadata synchronization relies on `resourcedetection` processor.
	//            If this option is enabled make sure that `resourcedetection` processor
	//            is enabled in the pipeline with one of the cloud provider detectors
	//            or environment variable detector setting a unique value to
	//            `host.name` attribute within your k8s cluster. Also keep override
	//            And keep `override=true` in resourcedetection config.
	SyncHostMetadata bool `mapstructure:"sync_host_metadata"`

	// ExcludeMetrics defines dpfilter.MetricFilters that will determine metrics to be
	// excluded from sending to SignalFx backend. If translations enabled with
	// TranslationRules options, the exclusion will be applie on translated metrics.
	ExcludeMetrics []dpfilters.MetricFilter `mapstructure:"exclude_metrics"`

	// IncludeMetrics defines dpfilter.MetricFilters to override exclusion any of metric.
	// This option can be used to included metrics that are otherwise dropped by default.
	// See ./translation/default_metrics.go for a list of metrics that are dropped by default.
	IncludeMetrics []dpfilters.MetricFilter `mapstructure:"include_metrics"`

	// Correlation configuration for syncing traces service and environment to metrics.
	Correlation *correlation.Config `mapstructure:"correlation"`

	// NonAlphanumericDimensionChars is a list of allowable characters, in addition to alphanumeric ones,
	// to be used in a dimension key.
	NonAlphanumericDimensionChars string `mapstructure:"nonalphanumeric_dimension_chars"`

	// MaxConnections is used to set a limit to the maximum idle HTTP connection the exporter can keep open.
	MaxConnections int `mapstructure:"max_connections"`
}

func (cfg *Config) getOptionsFromConfig() (*exporterOptions, error) {
	if err := cfg.validateConfig(); err != nil {
		return nil, err
	}

	ingestURL, err := cfg.getIngestURL()
	if err != nil {
		return nil, fmt.Errorf("invalid \"ingest_url\": %v", err)
	}

	apiURL, err := cfg.getAPIURL()
	if err != nil {
		return nil, fmt.Errorf("invalid \"api_url\": %v", err)
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}

	metricTranslator, err := translation.NewMetricTranslator(cfg.TranslationRules, cfg.DeltaTranslationTTL)
	if err != nil {
		return nil, fmt.Errorf("invalid \"%s\": %v", translationRulesConfigKey, err)
	}

	return &exporterOptions{
		ingestURL:        ingestURL,
		apiURL:           apiURL,
		httpTimeout:      cfg.Timeout,
		token:            cfg.AccessToken,
		logDataPoints:    cfg.LogDataPoints,
		logDimUpdate:     cfg.LogDimensionUpdates,
		metricTranslator: metricTranslator,
	}, nil
}

func (cfg *Config) validateConfig() error {
	if cfg.AccessToken == "" {
		return errors.New(`requires a non-empty "access_token"`)
	}

	if cfg.Realm == "" && (cfg.IngestURL == "" || cfg.APIURL == "") {
		return errors.New(`requires a non-empty "realm", or` +
			` "ingest_url" and "api_url" should be explicitly set`)
	}

	if cfg.Timeout < 0 {
		return errors.New(`cannot have a negative "timeout"`)
	}

	if cfg.MaxConnections < 0 {
		return errors.New(`cannot have a negative "max_connections"`)
	}

	return nil
}

func (cfg *Config) getIngestURL() (*url.URL, error) {
	if cfg.IngestURL != "" {
		// Ignore realm and use the IngestURL. Typically used for debugging.
		return url.Parse(cfg.IngestURL)
	}

	return url.Parse(fmt.Sprintf("https://ingest.%s.signalfx.com", cfg.Realm))
}

func (cfg *Config) getAPIURL() (*url.URL, error) {
	if cfg.APIURL != "" {
		// Ignore realm and use the APIURL. Typically used for debugging.
		return url.Parse(cfg.APIURL)
	}

	return url.Parse(fmt.Sprintf("https://api.%s.signalfx.com", cfg.Realm))
}

func (cfg *Config) Unmarshal(componentParser *config.Map) (err error) {
	if componentParser == nil {
		// Nothing to do if there is no config given.
		return nil
	}

	if err = componentParser.Unmarshal(cfg); err != nil {
		return err
	}

	// If translations_config is not set in the config, set it to the defaults and return.
	if !componentParser.IsSet(translationRulesConfigKey) {
		cfg.TranslationRules, err = loadDefaultTranslationRules()
		return err
	}

	return nil
}

func (cfg *Config) Validate() error {
	return nil
}
