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

package rabbitmqreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/rabbitmqreceiver"

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/rabbitmqreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/rabbitmqreceiver/internal/models"
)

const instrumentationLibraryName = "otelcol/rabbitmq"

var errClientNotInit = errors.New("client not initialized")

// Names of metrics in message_stats
const (
	deliverStat        = "deliver"
	publishStat        = "publish"
	ackStat            = "ack"
	dropUnroutableStat = "drop_unroutable"
)

// Metrics to gather from queue message_stats structure
var messageStatMetrics = []string{
	deliverStat,
	publishStat,
	ackStat,
	dropUnroutableStat,
}

// rabbitmqScraper handles scraping of RabbitMQ metrics
type rabbitmqScraper struct {
	client   client
	logger   *zap.Logger
	cfg      *Config
	settings component.TelemetrySettings
	mb       *metadata.MetricsBuilder
}

// newScraper creates a new scraper
func newScraper(logger *zap.Logger, cfg *Config, settings component.TelemetrySettings) *rabbitmqScraper {
	return &rabbitmqScraper{
		logger:   logger,
		cfg:      cfg,
		settings: settings,
		mb:       metadata.NewMetricsBuilder(cfg.Metrics),
	}
}

// start starts the scraper by creating a new HTTP Client on the scraper
func (r *rabbitmqScraper) start(ctx context.Context, host component.Host) (err error) {
	r.client, err = newClient(r.cfg, host, r.settings, r.logger)
	return
}

// scrape collects metrics from the RabbitMQ API
func (r *rabbitmqScraper) scrape(ctx context.Context) (pdata.Metrics, error) {
	metrics := pdata.NewMetrics()
	now := pdata.NewTimestampFromTime(time.Now())
	rms := metrics.ResourceMetrics()

	// Validate we don't attempt to scrape without initializing the client
	if r.client == nil {
		return metrics, errClientNotInit
	}

	// Get queues for processing
	queues, err := r.client.GetQueues(ctx)
	if err != nil {
		return metrics, err
	}

	// Collect metrics for each queue
	for _, queue := range queues {

		r.collectQueue(queue, now, rms)
	}

	return metrics, nil
}

// collectQueue collects metrics
func (r *rabbitmqScraper) collectQueue(queue *models.Queue, now pdata.Timestamp, rms pdata.ResourceMetricsSlice) {
	resourceMetric := rms.AppendEmpty()
	resourceAttrs := resourceMetric.Resource().Attributes()
	resourceAttrs.InsertString(metadata.A.RabbitmqQueueName, queue.Name)
	resourceAttrs.InsertString(metadata.A.RabbitmqNodeName, queue.Node)
	resourceAttrs.InsertString(metadata.A.RabbitmqVhostName, queue.VHost)

	ilms := resourceMetric.InstrumentationLibraryMetrics().AppendEmpty()
	ilms.InstrumentationLibrary().SetName(instrumentationLibraryName)

	r.mb.RecordRabbitmqConsumerCountDataPoint(now, queue.Consumers)
	r.mb.RecordRabbitmqMessageCurrentDataPoint(now, queue.UnacknowledgedMessages, metadata.AttributeMessageState.Unacknowledged)
	r.mb.RecordRabbitmqMessageCurrentDataPoint(now, queue.ReadyMessages, metadata.AttributeMessageState.Ready)

	for _, messageStatMetric := range messageStatMetrics {
		// Get metric value
		val, ok := queue.MessageStats[messageStatMetric]
		// A metric may not exist if the actions that increment it do not occur
		if !ok {
			r.logger.Debug("metric not found", zap.String("Metric", messageStatMetric), zap.String("Queue", queue.Name))
			continue
		}

		// Convert to int64
		val64, ok := convertValToInt64(val)
		if !ok {
			// Log warning if the metric is not in the format we expect
			r.logger.Warn("metric not int64", zap.String("Metric", messageStatMetric), zap.String("Queue", queue.Name))
			continue
		}

		switch messageStatMetric {
		case deliverStat:
			r.mb.RecordRabbitmqMessageDeliveredDataPoint(now, val64)
		case publishStat:
			r.mb.RecordRabbitmqMessagePublishedDataPoint(now, val64)
		case ackStat:
			r.mb.RecordRabbitmqMessageAcknowledgedDataPoint(now, val64)
		case dropUnroutableStat:
			r.mb.RecordRabbitmqMessageDroppedDataPoint(now, val64)

		}
	}
	r.mb.Emit(ilms.Metrics())
}

// convertValToInt64 values from message state unmarshal as float64s but should be int64.
// Need to do a double cast to get an int64.
// This should never fail but worth checking just in case.
func convertValToInt64(val interface{}) (int64, bool) {
	f64Val, ok := val.(float64)
	if !ok {
		return 0, ok
	}

	return int64(f64Val), true
}
