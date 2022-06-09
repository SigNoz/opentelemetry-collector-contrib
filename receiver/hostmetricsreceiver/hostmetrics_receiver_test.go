// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hostmetricsreceiver

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/model/pdata"
	conventions "go.opentelemetry.io/collector/model/semconv/v1.5.0"
	"go.opentelemetry.io/collector/receiver/scraperhelper"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/cpuscraper"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/diskscraper"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/filesystemscraper"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/loadscraper"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/memoryscraper"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/networkscraper"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/pagingscraper"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/processesscraper"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/processscraper"
)

var standardMetrics = []string{
	"system.cpu.time",
	"system.cpu.load_average.1m",
	"system.cpu.load_average.5m",
	"system.cpu.load_average.15m",
	"system.disk.io",
	"system.disk.io_time",
	"system.disk.operations",
	"system.disk.operation_time",
	"system.disk.pending_operations",
	"system.filesystem.usage",
	"system.memory.usage",
	"system.network.connections",
	"system.network.dropped",
	"system.network.errors",
	"system.network.io",
	"system.network.packets",
	"system.paging.operations",
	"system.paging.usage",
}

var resourceMetrics = []string{
	"process.cpu.time",
	"process.memory.physical_usage",
	"process.memory.virtual_usage",
	"process.disk.io",
}

var systemSpecificMetrics = map[string][]string{
	"linux":   {"system.disk.merged", "system.disk.weighted_io_time", "system.filesystem.inodes.usage", "system.paging.faults", "system.processes.created", "system.processes.count"},
	"darwin":  {"system.filesystem.inodes.usage", "system.paging.faults", "system.processes.count"},
	"freebsd": {"system.filesystem.inodes.usage", "system.paging.faults", "system.processes.count"},
	"openbsd": {"system.filesystem.inodes.usage", "system.paging.faults", "system.processes.created", "system.processes.count"},
	"solaris": {"system.filesystem.inodes.usage", "system.paging.faults"},
}

var factories = map[string]internal.ScraperFactory{
	cpuscraper.TypeStr:        &cpuscraper.Factory{},
	diskscraper.TypeStr:       &diskscraper.Factory{},
	filesystemscraper.TypeStr: &filesystemscraper.Factory{},
	loadscraper.TypeStr:       &loadscraper.Factory{},
	memoryscraper.TypeStr:     &memoryscraper.Factory{},
	networkscraper.TypeStr:    &networkscraper.Factory{},
	pagingscraper.TypeStr:     &pagingscraper.Factory{},
	processesscraper.TypeStr:  &processesscraper.Factory{},
	processscraper.TypeStr:    &processscraper.Factory{},
}

func TestGatherMetrics_EndToEnd(t *testing.T) {
	scraperFactories = factories

	sink := new(consumertest.MetricsSink)

	cfg := &Config{
		ScraperControllerSettings: scraperhelper.ScraperControllerSettings{
			ReceiverSettings:   config.NewReceiverSettings(config.NewComponentID(typeStr)),
			CollectionInterval: 100 * time.Millisecond,
		},
		Scrapers: map[string]internal.Config{
			cpuscraper.TypeStr:        scraperFactories[cpuscraper.TypeStr].CreateDefaultConfig(),
			diskscraper.TypeStr:       scraperFactories[diskscraper.TypeStr].CreateDefaultConfig(),
			filesystemscraper.TypeStr: (&filesystemscraper.Factory{}).CreateDefaultConfig(),
			loadscraper.TypeStr:       scraperFactories[loadscraper.TypeStr].CreateDefaultConfig(),
			memoryscraper.TypeStr:     scraperFactories[memoryscraper.TypeStr].CreateDefaultConfig(),
			networkscraper.TypeStr:    scraperFactories[networkscraper.TypeStr].CreateDefaultConfig(),
			pagingscraper.TypeStr:     &pagingscraper.Config{},
			processesscraper.TypeStr:  &processesscraper.Config{},
		},
	}

	if runtime.GOOS == "linux" || runtime.GOOS == "windows" {
		cfg.Scrapers[processscraper.TypeStr] = scraperFactories[processscraper.TypeStr].CreateDefaultConfig()
	}

	receiver, err := NewFactory().CreateMetricsReceiver(context.Background(), creationSet, cfg, sink)

	require.NoError(t, err, "Failed to create metrics receiver: %v", err)

	ctx, cancelFn := context.WithCancel(context.Background())
	err = receiver.Start(ctx, componenttest.NewNopHost())
	require.NoError(t, err, "Failed to start metrics receiver: %v", err)
	defer func() { assert.NoError(t, receiver.Shutdown(context.Background())) }()

	// canceling the context provided to Start should not cancel any async processes initiated by the receiver
	cancelFn()

	const tick = 50 * time.Millisecond
	const waitFor = 10 * time.Second
	require.Eventuallyf(t, func() bool {
		got := sink.AllMetrics()
		if len(got) == 0 {
			return false
		}

		assertIncludesExpectedMetrics(t, got[0])
		return true
	}, waitFor, tick, "No metrics were collected after %v", waitFor)
}

func assertIncludesExpectedMetrics(t *testing.T, got pdata.Metrics) {
	// get the superset of metrics returned by all resource metrics (excluding the first)
	returnedMetrics := make(map[string]struct{})
	returnedResourceMetrics := make(map[string]struct{})
	rms := got.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		metrics := getMetricSlice(t, rm)
		returnedMetricNames := getReturnedMetricNames(metrics)
		assert.EqualValues(t, conventions.SchemaURL, rm.SchemaUrl(),
			"SchemaURL is incorrect for metrics: %v", returnedMetricNames)
		if rm.Resource().Attributes().Len() == 0 {
			appendMapInto(returnedMetrics, returnedMetricNames)
		} else {
			appendMapInto(returnedResourceMetrics, returnedMetricNames)
		}
	}

	// verify the expected list of metrics returned (os dependent)
	var expectedMetrics []string
	expectedMetrics = append(expectedMetrics, standardMetrics...)
	expectedMetrics = append(expectedMetrics, systemSpecificMetrics[runtime.GOOS]...)
	assert.Equal(t, len(expectedMetrics), len(returnedMetrics))
	for _, expected := range expectedMetrics {
		assert.Contains(t, returnedMetrics, expected)
	}

	// verify the expected list of resource metrics returned (Linux & Windows only)
	if runtime.GOOS != "linux" && runtime.GOOS != "windows" {
		return
	}

	assert.Equal(t, len(resourceMetrics), len(returnedResourceMetrics))
	for _, expected := range resourceMetrics {
		assert.Contains(t, returnedResourceMetrics, expected)
	}
}

func getMetricSlice(t *testing.T, rm pdata.ResourceMetrics) pdata.MetricSlice {
	ilms := rm.InstrumentationLibraryMetrics()
	require.Equal(t, 1, ilms.Len())
	return ilms.At(0).Metrics()
}

func getReturnedMetricNames(metrics pdata.MetricSlice) map[string]struct{} {
	metricNames := make(map[string]struct{})
	for i := 0; i < metrics.Len(); i++ {
		metricNames[metrics.At(i).Name()] = struct{}{}
	}
	return metricNames
}

func appendMapInto(m1 map[string]struct{}, m2 map[string]struct{}) {
	for k, v := range m2 {
		m1[k] = v
	}
}

const mockTypeStr = "mock"

type mockConfig struct{}

type mockFactory struct{ mock.Mock }
type mockScraper struct{ mock.Mock }

func (m *mockFactory) CreateDefaultConfig() internal.Config { return &mockConfig{} }
func (m *mockFactory) CreateMetricsScraper(context.Context, *zap.Logger, internal.Config) (scraperhelper.Scraper, error) {
	args := m.MethodCalled("CreateMetricsScraper")
	return args.Get(0).(scraperhelper.Scraper), args.Error(1)
}

func (m *mockScraper) ID() config.ComponentID                      { return config.NewComponentID("") }
func (m *mockScraper) Start(context.Context, component.Host) error { return nil }
func (m *mockScraper) Shutdown(context.Context) error              { return nil }
func (m *mockScraper) Scrape(context.Context) (pdata.Metrics, error) {
	return pdata.NewMetrics(), errors.New("err1")
}

func TestGatherMetrics_ScraperKeyConfigError(t *testing.T) {
	scraperFactories = map[string]internal.ScraperFactory{}

	sink := new(consumertest.MetricsSink)
	cfg := &Config{Scrapers: map[string]internal.Config{"error": &mockConfig{}}}
	_, err := NewFactory().CreateMetricsReceiver(context.Background(), creationSet, cfg, sink)
	require.Error(t, err)
}

func TestGatherMetrics_CreateMetricsScraperError(t *testing.T) {
	mFactory := &mockFactory{}
	mFactory.On("CreateMetricsScraper").Return(&mockScraper{}, errors.New("err1"))
	scraperFactories = map[string]internal.ScraperFactory{mockTypeStr: mFactory}

	sink := new(consumertest.MetricsSink)
	cfg := &Config{Scrapers: map[string]internal.Config{mockTypeStr: &mockConfig{}}}
	_, err := NewFactory().CreateMetricsReceiver(context.Background(), creationSet, cfg, sink)
	require.Error(t, err)
}

type notifyingSink struct {
	receivedMetrics bool
	timesCalled     int
	ch              chan int
}

func (s *notifyingSink) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (s *notifyingSink) ConsumeMetrics(_ context.Context, md pdata.Metrics) error {
	if md.MetricCount() > 0 {
		s.receivedMetrics = true
	}

	s.timesCalled++
	s.ch <- s.timesCalled
	return nil
}

func benchmarkScrapeMetrics(b *testing.B, cfg *Config) {
	scraperFactories = factories

	sink := &notifyingSink{ch: make(chan int, 10)}
	tickerCh := make(chan time.Time)

	options, err := createAddScraperOptions(context.Background(), zap.NewNop(), cfg, scraperFactories)
	require.NoError(b, err)
	options = append(options, scraperhelper.WithTickerChannel(tickerCh))

	receiver, err := scraperhelper.NewScraperControllerReceiver(&cfg.ScraperControllerSettings, componenttest.NewNopReceiverCreateSettings(), sink, options...)
	require.NoError(b, err)

	require.NoError(b, receiver.Start(context.Background(), componenttest.NewNopHost()))

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		tickerCh <- time.Now()
		<-sink.ch
	}

	if !sink.receivedMetrics {
		b.Fail()
	}
}

func Benchmark_ScrapeCpuMetrics(b *testing.B) {
	cfg := &Config{
		ScraperControllerSettings: scraperhelper.DefaultScraperControllerSettings(""),
		Scrapers:                  map[string]internal.Config{cpuscraper.TypeStr: (&cpuscraper.Factory{}).CreateDefaultConfig()},
	}

	benchmarkScrapeMetrics(b, cfg)
}

func Benchmark_ScrapeDiskMetrics(b *testing.B) {
	cfg := &Config{
		ScraperControllerSettings: scraperhelper.DefaultScraperControllerSettings(""),
		Scrapers:                  map[string]internal.Config{diskscraper.TypeStr: (&diskscraper.Factory{}).CreateDefaultConfig()},
	}

	benchmarkScrapeMetrics(b, cfg)
}

func Benchmark_ScrapeFileSystemMetrics(b *testing.B) {
	cfg := &Config{
		ScraperControllerSettings: scraperhelper.DefaultScraperControllerSettings(""),
		Scrapers:                  map[string]internal.Config{filesystemscraper.TypeStr: (&filesystemscraper.Factory{}).CreateDefaultConfig()},
	}

	benchmarkScrapeMetrics(b, cfg)
}

func Benchmark_ScrapeLoadMetrics(b *testing.B) {
	cfg := &Config{
		ScraperControllerSettings: scraperhelper.DefaultScraperControllerSettings(""),
		Scrapers:                  map[string]internal.Config{loadscraper.TypeStr: (&loadscraper.Factory{}).CreateDefaultConfig()},
	}

	benchmarkScrapeMetrics(b, cfg)
}

func Benchmark_ScrapeMemoryMetrics(b *testing.B) {
	cfg := &Config{
		ScraperControllerSettings: scraperhelper.DefaultScraperControllerSettings(""),
		Scrapers:                  map[string]internal.Config{memoryscraper.TypeStr: (&memoryscraper.Factory{}).CreateDefaultConfig()},
	}

	benchmarkScrapeMetrics(b, cfg)
}

func Benchmark_ScrapeNetworkMetrics(b *testing.B) {
	cfg := &Config{
		ScraperControllerSettings: scraperhelper.DefaultScraperControllerSettings(""),
		Scrapers:                  map[string]internal.Config{networkscraper.TypeStr: (&networkscraper.Factory{}).CreateDefaultConfig()},
	}

	benchmarkScrapeMetrics(b, cfg)
}

func Benchmark_ScrapeProcessesMetrics(b *testing.B) {
	cfg := &Config{
		ScraperControllerSettings: scraperhelper.DefaultScraperControllerSettings(""),
		Scrapers:                  map[string]internal.Config{processesscraper.TypeStr: (&processesscraper.Factory{}).CreateDefaultConfig()},
	}

	benchmarkScrapeMetrics(b, cfg)
}

func Benchmark_ScrapePagingMetrics(b *testing.B) {
	cfg := &Config{
		ScraperControllerSettings: scraperhelper.DefaultScraperControllerSettings(""),
		Scrapers:                  map[string]internal.Config{pagingscraper.TypeStr: (&pagingscraper.Factory{}).CreateDefaultConfig()},
	}

	benchmarkScrapeMetrics(b, cfg)
}

func Benchmark_ScrapeProcessMetrics(b *testing.B) {
	if runtime.GOOS != "linux" && runtime.GOOS != "windows" {
		b.Skip("skipping test on non linux/windows")
	}

	cfg := &Config{
		ScraperControllerSettings: scraperhelper.DefaultScraperControllerSettings(""),
		Scrapers:                  map[string]internal.Config{processscraper.TypeStr: (&processscraper.Factory{}).CreateDefaultConfig()},
	}

	benchmarkScrapeMetrics(b, cfg)
}

func Benchmark_ScrapeSystemMetrics(b *testing.B) {
	cfg := &Config{
		ScraperControllerSettings: scraperhelper.DefaultScraperControllerSettings(""),
		Scrapers: map[string]internal.Config{
			cpuscraper.TypeStr:        (&cpuscraper.Factory{}).CreateDefaultConfig(),
			diskscraper.TypeStr:       (&diskscraper.Factory{}).CreateDefaultConfig(),
			filesystemscraper.TypeStr: (&filesystemscraper.Factory{}).CreateDefaultConfig(),
			loadscraper.TypeStr:       (&loadscraper.Factory{}).CreateDefaultConfig(),
			memoryscraper.TypeStr:     (&memoryscraper.Factory{}).CreateDefaultConfig(),
			networkscraper.TypeStr:    (&networkscraper.Factory{}).CreateDefaultConfig(),
			pagingscraper.TypeStr:     (&pagingscraper.Factory{}).CreateDefaultConfig(),
			processesscraper.TypeStr:  (&processesscraper.Factory{}).CreateDefaultConfig(),
		},
	}

	benchmarkScrapeMetrics(b, cfg)
}

func Benchmark_ScrapeSystemAndProcessMetrics(b *testing.B) {
	if runtime.GOOS != "linux" && runtime.GOOS != "windows" {
		b.Skip("skipping test on non linux/windows")
	}

	cfg := &Config{
		ScraperControllerSettings: scraperhelper.DefaultScraperControllerSettings(""),
		Scrapers: map[string]internal.Config{
			cpuscraper.TypeStr:        &cpuscraper.Config{},
			diskscraper.TypeStr:       &diskscraper.Config{},
			filesystemscraper.TypeStr: (&filesystemscraper.Factory{}).CreateDefaultConfig(),
			loadscraper.TypeStr:       &loadscraper.Config{},
			memoryscraper.TypeStr:     &memoryscraper.Config{},
			networkscraper.TypeStr:    &networkscraper.Config{},
			pagingscraper.TypeStr:     (&pagingscraper.Factory{}).CreateDefaultConfig(),
			processesscraper.TypeStr:  &processesscraper.Config{},
		},
	}

	benchmarkScrapeMetrics(b, cfg)
}
