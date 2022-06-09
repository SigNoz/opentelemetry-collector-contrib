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

package datadogexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter"

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"gopkg.in/zorkian/go-datadog-api.v2"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/internal/metrics"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/internal/model/translator"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/internal/scrub"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/internal/sketches"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/datadogexporter/internal/utils"
)

type metricsExporter struct {
	params   component.ExporterCreateSettings
	cfg      *config.Config
	ctx      context.Context
	client   *datadog.Client
	tr       *translator.Translator
	scrubber scrub.Scrubber
	retrier  *utils.Retrier
}

// assert `hostProvider` implements HostnameProvider interface
var _ translator.HostnameProvider = (*hostProvider)(nil)

type hostProvider struct {
	logger *zap.Logger
	cfg    *config.Config
}

func (p *hostProvider) Hostname(context.Context) (string, error) {
	return metadata.GetHost(p.logger, p.cfg), nil
}

// translatorFromConfig creates a new metrics translator from the exporter config.
func translatorFromConfig(logger *zap.Logger, cfg *config.Config) (*translator.Translator, error) {
	options := []translator.Option{
		translator.WithDeltaTTL(cfg.Metrics.DeltaTTL),
		translator.WithFallbackHostnameProvider(&hostProvider{logger, cfg}),
	}

	if cfg.Metrics.HistConfig.SendCountSum {
		options = append(options, translator.WithCountSumMetrics())
	}

	if cfg.Metrics.Quantiles {
		options = append(options, translator.WithQuantiles())
	}

	if cfg.Metrics.ExporterConfig.ResourceAttributesAsTags {
		options = append(options, translator.WithResourceAttributesAsTags())
	}

	if cfg.Metrics.ExporterConfig.InstrumentationLibraryMetadataAsTags {
		options = append(options, translator.WithInstrumentationLibraryMetadataAsTags())
	}

	options = append(options, translator.WithHistogramMode(translator.HistogramMode(cfg.Metrics.HistConfig.Mode)))

	var numberMode translator.NumberMode
	if cfg.Metrics.SendMonotonic {
		numberMode = translator.NumberModeCumulativeToDelta
	} else {
		numberMode = translator.NumberModeRawValue
	}
	options = append(options, translator.WithNumberMode(numberMode))

	return translator.New(logger, options...)
}

func newMetricsExporter(ctx context.Context, params component.ExporterCreateSettings, cfg *config.Config) (*metricsExporter, error) {
	client := utils.CreateClient(cfg.API.Key, cfg.Metrics.TCPAddr.Endpoint)
	client.ExtraHeader["User-Agent"] = utils.UserAgent(params.BuildInfo)
	client.HttpClient = utils.NewHTTPClient(cfg.TimeoutSettings, cfg.LimitedHTTPClientSettings)

	utils.ValidateAPIKey(params.Logger, client)

	tr, err := translatorFromConfig(params.Logger, cfg)
	if err != nil {
		return nil, err
	}

	scrubber := scrub.NewScrubber()
	return &metricsExporter{
		params:   params,
		cfg:      cfg,
		ctx:      ctx,
		client:   client,
		tr:       tr,
		scrubber: scrubber,
		retrier:  utils.NewRetrier(params.Logger, cfg.RetrySettings, scrubber),
	}, nil
}

func (exp *metricsExporter) pushSketches(ctx context.Context, sl sketches.SketchSeriesList) error {
	payload, err := sl.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal sketches: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx,
		http.MethodPost,
		exp.cfg.Metrics.TCPAddr.Endpoint+sketches.SketchSeriesEndpoint,
		bytes.NewBuffer(payload),
	)
	if err != nil {
		return fmt.Errorf("failed to build sketches HTTP request: %w", err)
	}

	utils.SetDDHeaders(req.Header, exp.params.BuildInfo, exp.cfg.API.Key)
	utils.SetExtraHeaders(req.Header, utils.ProtobufHeaders)
	resp, err := exp.client.HttpClient.Do(req)

	if err != nil {
		return fmt.Errorf("failed to do sketches HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("error when sending payload to %s: %s", sketches.SketchSeriesEndpoint, resp.Status)
	}
	return nil
}

func (exp *metricsExporter) PushMetricsDataScrubbed(ctx context.Context, md pdata.Metrics) error {
	return exp.scrubber.Scrub(exp.PushMetricsData(ctx, md))
}

func (exp *metricsExporter) PushMetricsData(ctx context.Context, md pdata.Metrics) error {

	// Start host metadata with resource attributes from
	// the first payload.
	if exp.cfg.SendMetadata {
		once := exp.cfg.OnceMetadata()
		once.Do(func() {
			attrs := pdata.NewAttributeMap()
			if md.ResourceMetrics().Len() > 0 {
				attrs = md.ResourceMetrics().At(0).Resource().Attributes()
			}
			go metadata.Pusher(exp.ctx, exp.params, exp.cfg, attrs)
		})
	}

	consumer := metrics.NewConsumer()
	pushTime := uint64(time.Now().UTC().UnixNano())
	err := exp.tr.MapMetrics(ctx, md, consumer)
	if err != nil {
		return fmt.Errorf("failed to map metrics: %w", err)
	}
	ms, sl := consumer.All(pushTime, exp.params.BuildInfo)
	metrics.ProcessMetrics(ms, exp.cfg)

	err = nil
	if len(ms) > 0 {
		err = multierr.Append(
			err,
			exp.retrier.DoWithRetries(ctx, func(context.Context) error {
				return exp.client.PostMetrics(ms)
			}),
		)
	}

	if len(sl) > 0 {
		err = multierr.Append(
			err,
			exp.retrier.DoWithRetries(ctx, func(ctx context.Context) error {
				return exp.pushSketches(ctx, sl)
			}),
		)
	}

	return err
}
