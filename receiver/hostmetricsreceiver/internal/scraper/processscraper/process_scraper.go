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

package processscraper // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/processscraper"

import (
	"context"
	"fmt"
	"time"

	"github.com/shirou/gopsutil/v3/host"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/model/pdata"
	conventions "go.opentelemetry.io/collector/model/semconv/v1.5.0"
	"go.opentelemetry.io/collector/receiver/scrapererror"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/processor/filterset"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/processscraper/internal/metadata"
)

const (
	cpuMetricsLen    = 1
	memoryMetricsLen = 2
	diskMetricsLen   = 1

	metricsLen = cpuMetricsLen + memoryMetricsLen + diskMetricsLen
)

// scraper for Process Metrics
type scraper struct {
	config    *Config
	mb        *metadata.MetricsBuilder
	includeFS filterset.FilterSet
	excludeFS filterset.FilterSet

	// for mocking
	bootTime          func() (uint64, error)
	getProcessHandles func() (processHandles, error)
}

// newProcessScraper creates a Process Scraper
func newProcessScraper(cfg *Config) (*scraper, error) {
	scraper := &scraper{config: cfg, bootTime: host.BootTime, getProcessHandles: getProcessHandlesInternal}

	var err error

	if len(cfg.Include.Names) > 0 {
		scraper.includeFS, err = filterset.CreateFilterSet(cfg.Include.Names, &cfg.Include.Config)
		if err != nil {
			return nil, fmt.Errorf("error creating process include filters: %w", err)
		}
	}

	if len(cfg.Exclude.Names) > 0 {
		scraper.excludeFS, err = filterset.CreateFilterSet(cfg.Exclude.Names, &cfg.Exclude.Config)
		if err != nil {
			return nil, fmt.Errorf("error creating process exclude filters: %w", err)
		}
	}

	return scraper, nil
}

func (s *scraper) start(context.Context, component.Host) error {
	bootTime, err := s.bootTime()
	if err != nil {
		return err
	}

	s.mb = metadata.NewMetricsBuilder(s.config.Metrics, metadata.WithStartTime(pdata.Timestamp(bootTime*1e9)))
	return nil
}

func (s *scraper) scrape(_ context.Context) (pdata.Metrics, error) {
	md := pdata.NewMetrics()
	rms := md.ResourceMetrics()

	var errs scrapererror.ScrapeErrors

	metadata, err := s.getProcessMetadata()
	if err != nil {
		partialErr, isPartial := err.(scrapererror.PartialScrapeError)
		if !isPartial {
			return md, err
		}

		errs.AddPartial(partialErr.Failed, partialErr)
	}

	rms.EnsureCapacity(len(metadata))
	for _, md := range metadata {
		rm := rms.AppendEmpty()
		rm.SetSchemaUrl(conventions.SchemaURL)
		md.initializeResource(rm.Resource())
		metrics := rm.InstrumentationLibraryMetrics().AppendEmpty().Metrics()

		now := pdata.NewTimestampFromTime(time.Now())

		if err = s.scrapeAndAppendCPUTimeMetric(now, md.handle); err != nil {
			errs.AddPartial(cpuMetricsLen, fmt.Errorf("error reading cpu times for process %q (pid %v): %w", md.executable.name, md.pid, err))
		}

		if err = s.scrapeAndAppendMemoryUsageMetrics(now, md.handle); err != nil {
			errs.AddPartial(memoryMetricsLen, fmt.Errorf("error reading memory info for process %q (pid %v): %w", md.executable.name, md.pid, err))
		}

		if err = s.scrapeAndAppendDiskIOMetric(now, md.handle); err != nil {
			errs.AddPartial(diskMetricsLen, fmt.Errorf("error reading disk usage for process %q (pid %v): %w", md.executable.name, md.pid, err))
		}
		s.mb.Emit(metrics)
	}

	return md, errs.Combine()
}

// getProcessMetadata returns a slice of processMetadata, including handles,
// for all currently running processes. If errors occur obtaining information
// for some processes, an error will be returned, but any processes that were
// successfully obtained will still be returned.
func (s *scraper) getProcessMetadata() ([]*processMetadata, error) {
	handles, err := s.getProcessHandles()
	if err != nil {
		return nil, err
	}

	var errs scrapererror.ScrapeErrors

	metadata := make([]*processMetadata, 0, handles.Len())
	for i := 0; i < handles.Len(); i++ {
		pid := handles.Pid(i)
		handle := handles.At(i)

		executable, err := getProcessExecutable(handle)
		if err != nil {
			if !s.config.MuteProcessNameError {
				errs.AddPartial(1, fmt.Errorf("error reading process name for pid %v: %w", pid, err))
			}
			continue
		}

		// filter processes by name
		if (s.includeFS != nil && !s.includeFS.Matches(executable.name)) ||
			(s.excludeFS != nil && s.excludeFS.Matches(executable.name)) {
			continue
		}

		command, err := getProcessCommand(handle)
		if err != nil {
			errs.AddPartial(0, fmt.Errorf("error reading command for process %q (pid %v): %w", executable.name, pid, err))
		}

		username, err := handle.Username()
		if err != nil {
			errs.AddPartial(0, fmt.Errorf("error reading username for process %q (pid %v): %w", executable.name, pid, err))
		}

		md := &processMetadata{
			pid:        pid,
			executable: executable,
			command:    command,
			username:   username,
			handle:     handle,
		}

		metadata = append(metadata, md)
	}

	return metadata, errs.Combine()
}

func (s *scraper) scrapeAndAppendCPUTimeMetric(now pdata.Timestamp, handle processHandle) error {
	times, err := handle.Times()
	if err != nil {
		return err
	}

	s.recordCPUTimeMetric(now, times)
	return nil
}

func (s *scraper) scrapeAndAppendMemoryUsageMetrics(now pdata.Timestamp, handle processHandle) error {
	mem, err := handle.MemoryInfo()
	if err != nil {
		return err
	}

	s.mb.RecordProcessMemoryPhysicalUsageDataPoint(now, int64(mem.RSS))
	s.mb.RecordProcessMemoryVirtualUsageDataPoint(now, int64(mem.VMS))
	return nil
}

func (s *scraper) scrapeAndAppendDiskIOMetric(now pdata.Timestamp, handle processHandle) error {
	io, err := handle.IOCounters()
	if err != nil {
		return err
	}

	s.mb.RecordProcessDiskIoDataPoint(now, int64(io.ReadBytes), metadata.AttributeDirection.Read)
	s.mb.RecordProcessDiskIoDataPoint(now, int64(io.WriteBytes), metadata.AttributeDirection.Write)
	return nil
}
