// Copyright 2017, 2018 Percona LLC
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

// Package clickhouse provides ClickHouse storage.
package clickhousemetricsexporter

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhousemetricsexporter/base"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhousemetricsexporter/utils/timeseries"
	"github.com/prometheus/prometheus/prompb"
)

const (
	signozMetricDBName = "signoz_metrics"
	signozSampleName   = "samples"
	signozTSName       = "time_series"
)

// clickHouse implements storage interface for the ClickHouse.
type clickHouse struct {
	db *sql.DB
	l  *logrus.Entry

	timeSeriesRW sync.RWMutex
	timeSeries   map[uint64]struct{}

	mWrittenTimeSeries prometheus.Counter
}

type ClickHouseParams struct {
	DSN          string
	DropDatabase bool
	MaxOpenConns int
}

func NewClickHouse(params *ClickHouseParams) (base.Storage, error) {
	l := logrus.WithField("component", "clickhouse")

	dsnURL, err := url.Parse(params.DSN)

	if err != nil {
		return nil, err
	}

	var queries []string
	if params.DropDatabase {
		queries = append(queries, fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, signozMetricDBName))
	}
	queries = append(queries, fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, signozMetricDBName))

	queries = append(queries, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			date Date Codec(DoubleDelta, LZ4),
			fingerprint UInt64 Codec(DoubleDelta, LZ4),
			labels LowCardinality(String) Codec(ZSTD(5))
		)
		ENGINE = ReplacingMergeTree
			PARTITION BY date
			ORDER BY fingerprint`, fmt.Sprintf("%s.%s", signozMetricDBName, signozTSName)))

	// change sampleRowSize is you change this table
	queries = append(queries, fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		metric_name LowCardinality(String) CODEC(ZSTD(1)), 
		fingerprint UInt64 Codec(DoubleDelta, LZ4),
		timestamp_ms Int64 Codec(DoubleDelta, LZ4),
		value Float64 Codec(Gorilla, LZ4)
	)
	ENGINE = MergeTree
		PARTITION BY toDate(timestamp_ms / 1000)
		ORDER BY (metric_name, fingerprint, timestamp_ms)`, fmt.Sprintf("%s.%s", signozMetricDBName, signozSampleName)))

	options := &clickhouse.Options{
		Addr: []string{dsnURL.Host},
	}
	if dsnURL.Query().Get("username") != "" {
		auth := clickhouse.Auth{
			// Database: "",
			Username: dsnURL.Query().Get("username"),
			Password: dsnURL.Query().Get("password"),
		}

		options.Auth = auth
	}
	// fmt.Println(options)
	initDB := clickhouse.OpenDB(options)

	initDB.SetConnMaxIdleTime(2)
	initDB.SetMaxOpenConns(params.MaxOpenConns)
	initDB.SetConnMaxLifetime(0)

	if err != nil {
		fmt.Println("Could not connect to clickhouse: ", err)
		return nil, err
	}

	for _, q := range queries {
		q = strings.TrimSpace(q)
		l.Infof("Executing: %s", q)
		if _, err = initDB.Exec(q); err != nil {
			fmt.Println("Error in executing query: ", err.Error())
			return nil, err
		}

	}

	ch := &clickHouse{
		db:         initDB,
		l:          l,
		timeSeries: make(map[uint64]struct{}, 1000000),

		mWrittenTimeSeries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "written_time_series",
			Help: "Number of written time series.",
		}),
	}

	go func() {
		ctx := pprof.WithLabels(context.TODO(), pprof.Labels("component", "clickhouse_reloader"))
		pprof.SetGoroutineLabels(ctx)
		ch.runTimeSeriesReloader(ctx)
	}()

	return ch, nil
}

func (ch *clickHouse) runTimeSeriesReloader(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	q := fmt.Sprintf(`SELECT DISTINCT fingerprint, labels FROM %s`, fmt.Sprintf("%s.%s", signozMetricDBName, signozTSName))
	for {
		ch.timeSeriesRW.RLock()
		timeSeries := make(map[uint64][]*prompb.Label, len(ch.timeSeries))
		ch.timeSeriesRW.RUnlock()

		err := func() error {
			ch.l.Debug(q)
			rows, err := ch.db.Query(q)
			if err != nil {
				return err
			}
			defer rows.Close()

			var f uint64
			var b []byte
			for rows.Next() {
				if err = rows.Scan(&f, &b); err != nil {
					return err
				}
				if timeSeries[f], err = unmarshalLabels(b); err != nil {
					return err
				}
			}
			return rows.Err()
		}()
		if err == nil {
			ch.timeSeriesRW.Lock()
			n := len(timeSeries) - len(ch.timeSeries)
			for f, _ := range timeSeries {
				ch.timeSeries[f] = struct{}{}
			}
			ch.timeSeriesRW.Unlock()
			ch.l.Debugf("Loaded %d existing time series, %d were unknown to this instance.", len(timeSeries), n)
		} else {
			ch.l.Error(err)
		}

		select {
		case <-ctx.Done():
			ch.l.Warn(ctx.Err())
			return
		case <-ticker.C:
		}
	}
}

func (ch *clickHouse) Describe(c chan<- *prometheus.Desc) {
	ch.mWrittenTimeSeries.Describe(c)
}

func (ch *clickHouse) Collect(c chan<- prometheus.Metric) {
	ch.mWrittenTimeSeries.Collect(c)
}

type beginTxer interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

func inTransaction(ctx context.Context, txer beginTxer, f func(*sql.Tx) error) (err error) {
	var tx *sql.Tx
	if tx, err = txer.BeginTx(ctx, nil); err != nil {
		err = errors.WithStack(err)
		return
	}
	defer func() {
		if err == nil {
			err = errors.WithStack(tx.Commit())
		} else {
			tx.Rollback()
		}
	}()
	err = f(tx)
	return
}

func getMetricNameFromLabels(labels *[]prompb.Label) (*string, error) {

	for _, label := range *labels {
		if label.Name == "__name__" {
			return &label.Value, nil
		}
	}
	return nil, fmt.Errorf("__name__ not found in labels of time series")
}

func (ch *clickHouse) Write(ctx context.Context, data *prompb.WriteRequest) error {
	// calculate fingerprints, map them to time series
	fingerprints := make([]uint64, len(data.Timeseries))
	timeSeries := make(map[uint64][]*prompb.Label, len(data.Timeseries))

	for i, ts := range data.Timeseries {
		labels := make([]*prompb.Label, len(ts.Labels))
		for j, label := range ts.Labels {
			labels[j] = &prompb.Label{
				Name:  label.Name,
				Value: label.Value,
			}

		}
		timeseries.SortLabels(labels)
		f := timeseries.Fingerprint(labels)
		fingerprints[i] = f
		timeSeries[f] = labels
	}
	if len(fingerprints) != len(timeSeries) {
		ch.l.Debugf("got %d fingerprints, but only %d of them were unique time series", len(fingerprints), len(timeSeries))
	}

	// find new time series
	var newTimeSeries []uint64
	ch.timeSeriesRW.Lock()
	for f, _ := range timeSeries {
		_, ok := ch.timeSeries[f]
		if !ok {
			newTimeSeries = append(newTimeSeries, f)
			ch.timeSeries[f] = struct{}{}
		}
	}
	ch.timeSeriesRW.Unlock()

	// write new time series
	if len(newTimeSeries) > 0 {
		err := inTransaction(ctx, ch.db, func(tx *sql.Tx) error {
			query := fmt.Sprintf(`INSERT INTO %s (date, fingerprint, labels) VALUES (?, ?, ?)`, fmt.Sprintf("%s.%s", signozMetricDBName, signozTSName))
			var stmt *sql.Stmt
			var err error
			if stmt, err = tx.PrepareContext(ctx, query); err != nil {
				return errors.WithStack(err)
			}

			args := make([]interface{}, 3)
			args[0] = model.Now().Time()
			for _, f := range newTimeSeries {
				args[1] = f
				args[2] = string(marshalLabels(timeSeries[f], make([]byte, 0, 128)))

				// ch.l.Infof("%s %v", query, args)

				if _, err := stmt.ExecContext(ctx, args...); err != nil {
					return errors.WithStack(err)
				}
			}

			return errors.WithStack(stmt.Close())
		})
		if err != nil {
			return err
		}
	}

	// write samples
	err := inTransaction(ctx, ch.db, func(tx *sql.Tx) error {
		query := fmt.Sprintf(`INSERT INTO %s (metric_name, fingerprint, timestamp_ms, value) VALUES (?, ?, ?)`, fmt.Sprintf("%s.%s", signozMetricDBName, signozSampleName))
		var stmt *sql.Stmt
		var err error
		if stmt, err = tx.PrepareContext(ctx, query); err != nil {
			return errors.WithStack(err)
		}

		args := make([]interface{}, 4)
		for i, ts := range data.Timeseries {
			args[0], err = getMetricNameFromLabels(&ts.Labels)
			if err != nil {
				return errors.WithStack(err)
			}
			args[1] = fingerprints[i]

			for _, s := range ts.Samples {
				args[2] = s.Timestamp
				args[3] = s.Value
				// ch.l.Debugf("%s %v", query, args)
				if _, err := stmt.ExecContext(ctx, args...); err != nil {
					return errors.WithStack(err)
				}
			}
		}

		return errors.WithStack(stmt.Close())
	})
	if err != nil {
		return err
	}

	n := len(newTimeSeries)
	if n != 0 {
		ch.mWrittenTimeSeries.Add(float64(n))
		ch.l.Debugf("Wrote %d new time series.", n)
	}
	return nil
}

// check interfaces
var (
	_ base.Storage = (*clickHouse)(nil)
)
