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

package prometheusremotewriteexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/prometheusremotewriteexporter"

import (
	"errors"
	"sort"
	"strings"
	"unicode"

	"github.com/prometheus/prometheus/prompb"
)

const (
	keyStr = "key"
)

// batchTimeSeries splits series into multiple batch write requests.
func batchTimeSeries(tsMap map[string]*prompb.TimeSeries, maxBatchByteSize int) ([]*prompb.WriteRequest, error) {
	if len(tsMap) == 0 {
		return nil, errors.New("invalid tsMap: cannot be empty map")
	}

	var requests []*prompb.WriteRequest
	var tsArray []prompb.TimeSeries
	sizeOfCurrentBatch := 0

	for _, v := range tsMap {
		sizeOfSeries := v.Size()

		if sizeOfCurrentBatch+sizeOfSeries >= maxBatchByteSize {
			wrapped := convertTimeseriesToRequest(tsArray)
			requests = append(requests, wrapped)

			tsArray = make([]prompb.TimeSeries, 0)
			sizeOfCurrentBatch = 0
		}

		tsArray = append(tsArray, *v)
		sizeOfCurrentBatch += sizeOfSeries
	}

	if len(tsArray) != 0 {
		wrapped := convertTimeseriesToRequest(tsArray)
		requests = append(requests, wrapped)
	}

	return requests, nil
}

func convertTimeseriesToRequest(tsArray []prompb.TimeSeries) *prompb.WriteRequest {
	// the remote_write endpoint only requires the timeseries.
	// otlp defines it's own way to handle metric metadata
	return &prompb.WriteRequest{
		// Prometheus requires time series to be sorted by Timestamp to avoid out of order problems.
		// See:
		// * https://github.com/open-telemetry/wg-prometheus/issues/10
		// * https://github.com/open-telemetry/opentelemetry-collector/issues/2315
		Timeseries: orderBySampleTimestamp(tsArray),
	}
}

func orderBySampleTimestamp(tsArray []prompb.TimeSeries) []prompb.TimeSeries {
	for i := range tsArray {
		sL := tsArray[i].Samples
		sort.Slice(sL, func(i, j int) bool {
			return sL[i].Timestamp < sL[j].Timestamp
		})
	}
	return tsArray
}

// copied from prometheus-go-metric-exporter
// sanitize replaces non-alphanumeric characters with underscores in s.
func sanitize(s string) string {
	if len(s) == 0 {
		return s
	}

	// Note: No length limit for label keys because Prometheus doesn't
	// define a length limit, thus we should NOT be truncating label keys.
	// See https://github.com/orijtech/prometheus-go-metrics-exporter/issues/4.
	s = strings.Map(sanitizeRune, s)
	if unicode.IsDigit(rune(s[0])) {
		s = keyStr + "_" + s
	}
	if s[0] == '_' {
		s = keyStr + s
	}
	return s
}

func sanitizeLabels(s string) string {
	if len(s) == 0 {
		return s
	}

	// Note: No length limit for label keys because Prometheus doesn't
	// define a length limit, thus we should NOT be truncating label keys.
	// labels that start with _ are not sanitized
	s = strings.Map(sanitizeRune, s)
	if unicode.IsDigit(rune(s[0])) {
		s = keyStr + "_" + s
	}
	return s
}

// copied from prometheus-go-metric-exporter
// sanitizeRune converts anything that is not a letter or digit to an underscore
func sanitizeRune(r rune) rune {
	if unicode.IsLetter(r) || unicode.IsDigit(r) {
		return r
	}
	// Everything else turns into an underscore
	return '_'
}
