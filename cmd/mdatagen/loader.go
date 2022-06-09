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

package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-playground/locales/en"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	"github.com/go-playground/validator/v10/non-standard/validators"
	en_translations "github.com/go-playground/validator/v10/translations/en"
	"go.opentelemetry.io/collector/config/configmapprovider"
)

type metricName string

func (mn metricName) Render() (string, error) {
	return formatIdentifier(string(mn), true)
}

func (mn metricName) RenderUnexported() (string, error) {
	return formatIdentifier(string(mn), false)
}

type attributeName string

func (mn attributeName) Render() (string, error) {
	return formatIdentifier(string(mn), true)
}

func (mn attributeName) RenderUnexported() (string, error) {
	return formatIdentifier(string(mn), false)
}

type metric struct {
	// Enabled defines whether the metric is enabled by default.
	Enabled *bool `yaml:"enabled" validate:"required"`

	// Description of the metric.
	Description string `validate:"required,notblank"`

	// ExtendedDocumentation of the metric. If specified, this will
	// be appended to the description used in generated documentation.
	ExtendedDocumentation string `mapstructure:"extended_documentation"`

	// Unit of the metric.
	Unit string `mapstructure:"unit"`

	// Sum stores metadata for sum metric type
	Sum *sum `yaml:"sum"`
	// Gauge stores metadata for gauge metric type
	Gauge *gauge `yaml:"gauge"`
	// Histogram stores metadata for histogram metric type
	Histogram *histogram `yaml:"histogram"`

	// Attributes is the list of attributes that the metric emits.
	Attributes []attributeName
}

func (m metric) Data() MetricData {
	if m.Sum != nil {
		return m.Sum
	}
	if m.Gauge != nil {
		return m.Gauge
	}
	if m.Histogram != nil {
		return m.Histogram
	}
	return nil
}

func (m metric) IsEnabled() bool {
	return *m.Enabled
}

type attribute struct {
	// Description describes the purpose of the attribute.
	Description string `validate:"notblank"`
	// Value can optionally specify the value this attribute will have.
	// For example, the attribute may have the identifier `MemState` to its
	// value may be `state` when used.
	Value string
	// Enum can optionally describe the set of values to which the attribute can belong.
	Enum []string
}

type metadata struct {
	// Name of the component.
	Name string `validate:"notblank"`
	// Attributes emitted by one or more metrics.
	Attributes map[attributeName]attribute `validate:"dive"`
	// Metrics that can be emitted by the component.
	Metrics map[metricName]metric `validate:"dive"`
}

type templateContext struct {
	metadata
	// Package name for generated code.
	Package string
	// ExpGen identifies whether the experimental metrics generator is used.
	// TODO: Remove once the old mdata generator is gone.
	ExpGen bool
}

func loadMetadata(filePath string) (metadata, error) {
	cp, err := configmapprovider.NewFile().Retrieve(context.Background(), "file:"+filePath, nil)
	if err != nil {
		return metadata{}, err
	}

	var md metadata
	if err := cp.Map.UnmarshalExact(&md); err != nil {
		return metadata{}, err
	}

	if err := validateMetadata(md); err != nil {
		return md, err
	}

	return md, nil
}

func validateMetadata(out metadata) error {
	v := validator.New()
	if err := v.RegisterValidation("notblank", validators.NotBlank); err != nil {
		return fmt.Errorf("failed registering notblank validator: %v", err)
	}

	// Provides better validation error messages.
	enLocale := en.New()
	uni := ut.New(enLocale, enLocale)

	tr, ok := uni.GetTranslator("en")
	if !ok {
		return errors.New("unable to lookup en translator")
	}

	if err := en_translations.RegisterDefaultTranslations(v, tr); err != nil {
		return fmt.Errorf("failed registering translations: %v", err)
	}

	if err := v.RegisterTranslation("nosuchattribute", tr, func(ut ut.Translator) error {
		return ut.Add("nosuchattribute", "unknown attribute value", true) // see universal-translator for details
	}, func(ut ut.Translator, fe validator.FieldError) string {
		t, _ := ut.T("nosuchattribute", fe.Field())
		return t
	}); err != nil {
		return fmt.Errorf("failed registering nosuchattribute: %v", err)
	}

	v.RegisterStructValidation(metricValidation, metric{})

	if err := v.Struct(&out); err != nil {
		if verr, ok := err.(validator.ValidationErrors); ok {
			m := verr.Translate(tr)
			buf := strings.Builder{}
			buf.WriteString("error validating struct:\n")
			for k, v := range m {
				buf.WriteString(fmt.Sprintf("\t%v: %v\n", k, v))
			}
			return errors.New(buf.String())
		}
		return fmt.Errorf("unknown validation error: %v", err)
	}

	// Set metric data interface.
	for k, v := range out.Metrics {
		dataTypesSet := 0
		if v.Sum != nil {
			dataTypesSet++
		}
		if v.Gauge != nil {
			dataTypesSet++
		}
		if v.Histogram != nil {
			dataTypesSet++
		}
		if dataTypesSet == 0 {
			return fmt.Errorf("metric %v doesn't have a metric type key, "+
				"one of the following has to be specified: sum, gauge, histogram", k)
		}
		if dataTypesSet > 1 {
			return fmt.Errorf("metric %v has more than one metric type keys, "+
				"only one of the following has to be specified: sum, gauge, histogram", k)
		}
	}

	return nil
}

// metricValidation validates metric structs.
func metricValidation(sl validator.StructLevel) {
	// Make sure that the attributes are valid.
	md := sl.Top().Interface().(*metadata)
	cur := sl.Current().Interface().(metric)

	for _, l := range cur.Attributes {
		if _, ok := md.Attributes[l]; !ok {
			sl.ReportError(cur.Attributes, fmt.Sprintf("Attributes[%s]", string(l)), "Attributes", "nosuchattribute",
				"")
		}
	}
}
