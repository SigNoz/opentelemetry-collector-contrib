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

package attributesprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributesprocessor"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/attraction"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/processor/filterlog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/processor/filterspan"
)

const (
	// typeStr is the value of "type" key in configuration.
	typeStr = "attributes"
)

var processorCapabilities = consumer.Capabilities{MutatesData: true}

// NewFactory returns a new factory for the Attributes processor.
func NewFactory() component.ProcessorFactory {
	return processorhelper.NewFactory(
		typeStr,
		createDefaultConfig,
		processorhelper.WithTraces(createTracesProcessor),
		processorhelper.WithLogs(createLogProcessor))
}

// Note: This isn't a valid configuration because the processor would do no work.
func createDefaultConfig() config.Processor {
	return &Config{
		ProcessorSettings: config.NewProcessorSettings(config.NewComponentID(typeStr)),
	}
}

func createTracesProcessor(
	_ context.Context,
	_ component.ProcessorCreateSettings,
	cfg config.Processor,
	nextConsumer consumer.Traces,
) (component.TracesProcessor, error) {
	oCfg := cfg.(*Config)
	if len(oCfg.Actions) == 0 {
		return nil, fmt.Errorf("error creating \"attributes\" processor due to missing required field \"actions\" of processor %v", cfg.ID())
	}
	attrProc, err := attraction.NewAttrProc(&oCfg.Settings)
	if err != nil {
		return nil, fmt.Errorf("error creating \"attributes\" processor: %w of processor %v", err, cfg.ID())
	}
	include, err := filterspan.NewMatcher(oCfg.Include)
	if err != nil {
		return nil, err
	}
	exclude, err := filterspan.NewMatcher(oCfg.Exclude)
	if err != nil {
		return nil, err
	}

	return processorhelper.NewTracesProcessor(
		cfg,
		nextConsumer,
		newSpanAttributesProcessor(attrProc, include, exclude).processTraces,
		processorhelper.WithCapabilities(processorCapabilities))
}

func createLogProcessor(
	_ context.Context,
	set component.ProcessorCreateSettings,
	cfg config.Processor,
	nextConsumer consumer.Logs,
) (component.LogsProcessor, error) {
	oCfg := cfg.(*Config)
	if len(oCfg.Actions) == 0 {
		return nil, fmt.Errorf("error creating \"attributes\" processor due to missing required field \"actions\" of processor %v", cfg.ID())
	}
	attrProc, err := attraction.NewAttrProc(&oCfg.Settings)
	if err != nil {
		return nil, fmt.Errorf("error creating \"attributes\" processor: %w of processor %v", err, cfg.ID())
	}

	if (oCfg.Include != nil && len(oCfg.Include.LogNames) > 0) || (oCfg.Exclude != nil && len(oCfg.Exclude.LogNames) > 0) {
		set.Logger.Warn("log_names setting is deprecated and will be removed soon")
	}

	include, err := filterlog.NewMatcher(oCfg.Include)
	if err != nil {
		return nil, err
	}
	exclude, err := filterlog.NewMatcher(oCfg.Exclude)
	if err != nil {
		return nil, err
	}

	return processorhelper.NewLogsProcessor(
		cfg,
		nextConsumer,
		newLogAttributesProcessor(attrProc, include, exclude).processLogs,
		processorhelper.WithCapabilities(processorCapabilities))
}
