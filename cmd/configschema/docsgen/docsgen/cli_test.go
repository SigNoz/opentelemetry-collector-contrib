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

package docsgen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"text/template"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/loggingexporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/extension/ballastextension"
	"go.opentelemetry.io/collector/extension/zpagesextension"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/processor/memorylimiterprocessor"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.uber.org/multierr"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/configschema"
)

func TestWriteConfigDoc(t *testing.T) {
	cfg := otlpreceiver.NewFactory().CreateDefaultConfig()
	root := filepath.Join("..", "..", "..", "..")
	dr := configschema.NewDirResolver(root, configschema.DefaultModule)
	outputFilename := ""
	tmpl := testTemplate(t)
	writeConfigDoc(tmpl, dr, configschema.CfgInfo{
		Type:        "otlp",
		Group:       "receiver",
		CfgInstance: cfg,
	}, func(dir string, bytes []byte, perm os.FileMode) error {
		outputFilename = dir
		return nil
	})
	expectedPath := "receiver/otlpreceiver/config.md"
	assert.True(t, strings.HasSuffix(outputFilename, expectedPath))
}

func testTemplate(t *testing.T) *template.Template {
	tmpl, err := template.ParseFiles("testdata/test.tmpl")
	require.NoError(t, err)
	return tmpl
}

func TestHandleCLI_NoArgs(t *testing.T) {
	wr := &fakeIOWriter{}
	handleCLI(
		defaultComponents(t),
		configschema.NewDefaultDirResolver(),
		testTemplate(t),
		func(filename string, data []byte, perm os.FileMode) error { return nil },
		wr,
	)
	assert.Equal(t, 3, len(wr.lines))
}

func TestHandleCLI_Single(t *testing.T) {
	args := []string{"", "receiver", "otlp"}
	cs := defaultComponents(t)
	wr := &fakeFilesystemWriter{}

	testHandleCLI(t, cs, wr, args)

	assert.Equal(t, 1, len(wr.configFiles))
	assert.Equal(t, 1, len(wr.fileContents))
	assert.True(t, strings.Contains(wr.fileContents[0], `"otlp" Receiver Reference`))
}

func TestHandleCLI_All(t *testing.T) {
	args := []string{"", "all"}
	cs := defaultComponents(t)
	wr := &fakeFilesystemWriter{}

	testHandleCLI(t, cs, wr, args)

	expected := len(cs.Receivers) + len(cs.Processors) + len(cs.Exporters) + len(cs.Extensions)
	assert.Equal(t, expected, len(wr.configFiles))
	assert.Equal(t, expected, len(wr.fileContents))
}

func testHandleCLI(t *testing.T, cs component.Factories, wr *fakeFilesystemWriter, args []string) {
	stdoutWriter := &fakeIOWriter{}
	tmpl := testTemplate(t)
	dr := configschema.NewDirResolver(filepath.Join("..", "..", "..", ".."), configschema.DefaultModule)
	handleCLI(cs, dr, tmpl, wr.writeFile, stdoutWriter, args...)
}

func defaultComponents(t *testing.T) component.Factories {
	var errs error

	extensions, err := component.MakeExtensionFactoryMap(
		zpagesextension.NewFactory(),
		ballastextension.NewFactory(),
	)
	errs = multierr.Append(errs, err)

	receivers, err := component.MakeReceiverFactoryMap(
		otlpreceiver.NewFactory(),
	)
	errs = multierr.Append(errs, err)

	exporters, err := component.MakeExporterFactoryMap(
		loggingexporter.NewFactory(),
		otlpexporter.NewFactory(),
		otlphttpexporter.NewFactory(),
	)
	errs = multierr.Append(errs, err)

	processors, err := component.MakeProcessorFactoryMap(
		batchprocessor.NewFactory(),
		memorylimiterprocessor.NewFactory(),
	)
	errs = multierr.Append(errs, err)

	cmps := component.Factories{
		Extensions: extensions,
		Receivers:  receivers,
		Processors: processors,
		Exporters:  exporters,
	}
	require.NoError(t, errs)
	return cmps
}

type fakeFilesystemWriter struct {
	configFiles, fileContents []string
}

func (wr *fakeFilesystemWriter) writeFile(filename string, data []byte, perm os.FileMode) error {
	wr.configFiles = append(wr.configFiles, filename)
	wr.fileContents = append(wr.fileContents, string(data))
	return nil
}

type fakeIOWriter struct {
	lines []string
}

func (wr *fakeIOWriter) Write(p []byte) (n int, err error) {
	wr.lines = append(wr.lines, string(p))
	return 0, nil
}
