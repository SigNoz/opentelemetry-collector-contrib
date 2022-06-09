// Copyright 2020, OpenTelemetry Authors
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

package dimensions // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/signalfxexporter/internal/dimensions"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/signalfxexporter/internal/translation"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/sanitize"
)

// DimensionClient sends updates to dimensions to the SignalFx API
// This is a port of https://github.com/signalfx/signalfx-agent/blob/main/pkg/core/writer/dimensions/client.go
// with the only major difference being deduplication of dimension
// updates are currently not done by this port.
type DimensionClient struct {
	sync.RWMutex
	ctx           context.Context
	Token         string
	APIURL        *url.URL
	client        *http.Client
	requestSender *ReqSender
	// How long to wait for property updates to be sent once they are
	// generated.  Any duplicate updates to the same dimension within this time
	// frame will result in the latest property set being sent.  This helps
	// prevent spurious updates that get immediately overwritten by very flappy
	// property generation.
	sendDelay time.Duration
	// Set of dims that have been queued up for sending.  Use map to quickly
	// look up in case we need to replace due to flappy prop generation.
	delayedSet map[DimensionKey]*DimensionUpdate
	// Queue of dimensions to update.  The ordering should never change once
	// put in the queue so no need for heap/priority queue.
	delayedQueue chan *queuedDimension
	// For easier unit testing
	now func() time.Time

	// TODO: Look into collecting these metrics and other traces via obsreport
	DimensionsCurrentlyDelayed int64
	TotalDimensionsDropped     int64
	// The number of dimension updates that happened to the same dimension
	// within sendDelay.
	TotalFlappyUpdates           int64
	TotalClientError4xxResponses int64
	TotalRetriedUpdates          int64
	TotalInvalidDimensions       int64
	TotalSuccessfulUpdates       int64
	logUpdates                   bool
	logger                       *zap.Logger
	metricsConverter             translation.MetricsConverter
}

type queuedDimension struct {
	*DimensionUpdate
	TimeToSend time.Time
}

type DimensionClientOptions struct {
	Token                 string
	APIURL                *url.URL
	LogUpdates            bool
	Logger                *zap.Logger
	SendDelay             int
	PropertiesMaxBuffered int
	MetricsConverter      translation.MetricsConverter
}

// NewDimensionClient returns a new client
func NewDimensionClient(ctx context.Context, options DimensionClientOptions) *DimensionClient {
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext,
			MaxIdleConns:        20,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     30 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}
	sender := NewReqSender(ctx, client, 20, map[string]string{"client": "dimension"})

	return &DimensionClient{
		ctx:              ctx,
		Token:            options.Token,
		APIURL:           options.APIURL,
		sendDelay:        time.Duration(options.SendDelay) * time.Second,
		delayedSet:       make(map[DimensionKey]*DimensionUpdate),
		delayedQueue:     make(chan *queuedDimension, options.PropertiesMaxBuffered),
		requestSender:    sender,
		client:           client,
		now:              time.Now,
		logger:           options.Logger,
		logUpdates:       options.LogUpdates,
		metricsConverter: options.MetricsConverter,
	}
}

// Start the client's processing queue
func (dc *DimensionClient) Start() {
	go dc.processQueue()
}

// acceptDimension to be sent to the API.  This will return fairly quickly and
// won't block. If the buffer is full, the dim update will be dropped.
func (dc *DimensionClient) acceptDimension(dimUpdate *DimensionUpdate) error {
	dc.Lock()
	defer dc.Unlock()

	if delayedDimUpdate := dc.delayedSet[dimUpdate.Key()]; delayedDimUpdate != nil {
		if !reflect.DeepEqual(delayedDimUpdate, dimUpdate) {
			dc.TotalFlappyUpdates++

			// Merge the latest updates into existing one.
			delayedDimUpdate.Properties = mergeProperties(delayedDimUpdate.Properties, dimUpdate.Properties)
			delayedDimUpdate.Tags = mergeTags(delayedDimUpdate.Tags, dimUpdate.Tags)
		}
	} else {
		atomic.AddInt64(&dc.DimensionsCurrentlyDelayed, int64(1))

		dc.delayedSet[dimUpdate.Key()] = dimUpdate
		select {
		case dc.delayedQueue <- &queuedDimension{
			DimensionUpdate: dimUpdate,
			TimeToSend:      dc.now().Add(dc.sendDelay),
		}:
			break
		default:
			dc.TotalDimensionsDropped++
			atomic.AddInt64(&dc.DimensionsCurrentlyDelayed, int64(-1))
			return errors.New("dropped dimension update, propertiesMaxBuffered exceeded")
		}
	}

	return nil
}

// mergeProperties merges 2 or more maps of properties. This method gives
// precedence to values of properties in later maps. i.e., if more than one
// map has the same key, the last value seen will be the effective value in
// the output.
func mergeProperties(propMaps ...map[string]*string) map[string]*string {
	out := map[string]*string{}
	for _, propMap := range propMaps {
		for k, v := range propMap {
			out[k] = v
		}
	}
	return out
}

// mergeTags merges 2 or more sets of tags. This method gives precedence to
// tags seen in later sets. i.e., if more than one set has the same tag, the
// last value seen will be the effective value in the output.
func mergeTags(tagSets ...map[string]bool) map[string]bool {
	out := map[string]bool{}
	for _, tagSet := range tagSets {
		for k, v := range tagSet {
			out[k] = v
		}
	}
	return out
}

func (dc *DimensionClient) processQueue() {
	for {
		select {
		case <-dc.ctx.Done():
			return
		case delayedDimUpdate := <-dc.delayedQueue:
			now := dc.now()
			if now.Before(delayedDimUpdate.TimeToSend) {
				// dims are always in the channel in order of TimeToSend
				time.Sleep(delayedDimUpdate.TimeToSend.Sub(now))
			}

			atomic.AddInt64(&dc.DimensionsCurrentlyDelayed, int64(-1))

			dc.Lock()
			delete(dc.delayedSet, delayedDimUpdate.Key())
			dc.Unlock()

			if err := dc.handleDimensionUpdate(delayedDimUpdate.DimensionUpdate); err != nil {
				dc.logger.Error(
					"Could not send dimension update",
					zap.Error(err),
					zap.String("dimensionUpdate", delayedDimUpdate.String()),
				)
			}
		}
	}
}

// handleDimensionUpdate will set custom properties on a specific dimension value.
func (dc *DimensionClient) handleDimensionUpdate(dimUpdate *DimensionUpdate) error {
	var (
		req *http.Request
		err error
	)

	req, err = dc.makePatchRequest(dimUpdate)

	if err != nil {
		return err
	}

	req = req.WithContext(
		context.WithValue(req.Context(), RequestFailedCallbackKey, RequestFailedCallback(func(statusCode int, err error) {
			if statusCode >= 400 && statusCode < 500 && statusCode != 404 {
				atomic.AddInt64(&dc.TotalClientError4xxResponses, int64(1))
				dc.logger.Error(
					"Unable to update dimension, not retrying",
					zap.Error(err),
					zap.String("URL", sanitize.URL(req.URL)),
					zap.String("dimensionUpdate", dimUpdate.String()),
					zap.Int("statusCode", statusCode),
				)

				// Don't retry if it is a 4xx error (except 404) since these
				// imply an input/auth error, which is not going to be remedied
				// by retrying.
				// 404 errors are special because they can occur due to races
				// within the dimension patch endpoint.
				return
			}

			dc.logger.Error(
				"Unable to update dimension, retrying",
				zap.Error(err),
				zap.String("URL", sanitize.URL(req.URL)),
				zap.String("dimensionUpdate", dimUpdate.String()),
				zap.Int("statusCode", statusCode),
			)
			atomic.AddInt64(&dc.TotalRetriedUpdates, int64(1))
			// The retry is meant to provide some measure of robustness against
			// temporary API failures.  If the API is down for significant
			// periods of time, dimension updates will probably eventually back
			// up beyond PropertiesMaxBuffered and start dropping.
			if err := dc.acceptDimension(dimUpdate); err != nil {
				dc.logger.Error(
					"Failed to retry dimension update",
					zap.Error(err),
					zap.String("URL", sanitize.URL(req.URL)),
					zap.String("dimensionUpdate", dimUpdate.String()),
					zap.Int("statusCode", statusCode),
				)
			}
		})))

	req = req.WithContext(
		context.WithValue(req.Context(), RequestSuccessCallbackKey, RequestSuccessCallback(func([]byte) {
			if dc.logUpdates {
				dc.logger.Info(
					"Updated dimension",
					zap.String("dimensionUpdate", dimUpdate.String()),
				)
			}
		})))

	dc.requestSender.Send(req)

	return nil
}

func (dc *DimensionClient) makeDimURL(key, value string) (*url.URL, error) {
	url, err := dc.APIURL.Parse(fmt.Sprintf("/v2/dimension/%s/%s", url.PathEscape(key), url.PathEscape(value)))
	if err != nil {
		return nil, fmt.Errorf("could not construct dimension property PATCH URL with %s / %s: %v", key, value, err)
	}

	return url, nil
}

func (dc *DimensionClient) makePatchRequest(dim *DimensionUpdate) (*http.Request, error) {
	var (
		tagsToAdd    []string
		tagsToRemove []string
	)

	for tag, shouldAdd := range dim.Tags {
		if shouldAdd {
			tagsToAdd = append(tagsToAdd, tag)
		} else {
			tagsToRemove = append(tagsToRemove, tag)
		}
	}

	json, err := json.Marshal(map[string]interface{}{
		"customProperties": dim.Properties,
		"tags":             tagsToAdd,
		"tagsToRemove":     tagsToRemove,
	})
	if err != nil {
		return nil, err
	}

	url, err := dc.makeDimURL(dim.Name, dim.Value)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(
		"PATCH",
		strings.TrimRight(url.String(), "/")+"/_/sfxagent",
		bytes.NewReader(json))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-SF-TOKEN", dc.Token)

	return req, nil
}
