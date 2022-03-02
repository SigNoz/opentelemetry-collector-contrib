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

package clickhouseexporter

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"

	"go.uber.org/zap"
)

type Encoding string

const (
	// EncodingJSON is used for spans encoded as JSON.
	EncodingJSON Encoding = "json"
	// EncodingProto is used for spans encoded as Protobuf.
	EncodingProto Encoding = "protobuf"
)

// SpanWriter for writing spans to ClickHouse
type SpanWriter struct {
	logger     *zap.Logger
	db         *sqlx.DB
	indexTable string
	errorTable string
	encoding   Encoding
	delay      time.Duration
	size       int
	spans      chan *Span
	finish     chan bool
	done       sync.WaitGroup
}

// NewSpanWriter returns a SpanWriter for the database
func NewSpanWriter(logger *zap.Logger, db *sqlx.DB, indexTable string, errorTable string, encoding Encoding, delay time.Duration, size int) *SpanWriter {
	writer := &SpanWriter{
		logger:     logger,
		db:         db,
		indexTable: indexTable,
		errorTable: errorTable,
		encoding:   encoding,
		delay:      delay,
		size:       size,
		spans:      make(chan *Span, size),
		finish:     make(chan bool),
	}

	go writer.backgroundWriter()

	return writer
}

func (w *SpanWriter) backgroundWriter() {
	batch := make([]*Span, 0, w.size)

	timer := time.After(w.delay)
	last := time.Now()

	for {
		w.done.Add(1)

		flush := false
		finish := false

		select {
		case span := <-w.spans:
			batch = append(batch, span)
			flush = len(batch) == cap(batch)
		case <-timer:
			timer = time.After(w.delay)
			flush = time.Since(last) > w.delay && len(batch) > 0
		case <-w.finish:
			finish = true
			flush = len(batch) > 0
		}

		if flush {
			if err := w.writeBatch(batch); err != nil {
				w.logger.Error("Could not write a batch of spans", zap.Error(err))
			}

			batch = make([]*Span, 0, w.size)
			last = time.Now()
		}

		w.done.Done()

		if finish {
			break
		}
	}
}

func (w *SpanWriter) writeBatch(batch []*Span) error {

	if w.indexTable != "" {
		if err := w.writeIndexBatch(batch); err != nil {
			return err
		}
	}
	if w.errorTable != "" {
		if err := w.writeErrorBatch(batch); err != nil {
			return err
		}
	}

	return nil
}

func (w *SpanWriter) writeIndexBatch(batch []*Span) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}

	commited := false

	defer func() {
		if !commited {
			// Clickhouse does not support real rollback
			_ = tx.Rollback()
		}
	}()

	statement, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s (timestamp, traceID, spanID, parentSpanID, serviceName, name, kind, durationNano, tags, tagsKeys, tagsValues, statusCode, references, externalHttpMethod, externalHttpUrl, component, dbSystem, dbName, dbOperation, peerService, events, httpUrl, httpMethod, httpHost, httpRoute, httpCode, msgSystem, msgOperation, hasError) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", w.indexTable))
	if err != nil {
		return err
	}

	defer statement.Close()

	for _, span := range batch {
		_, err = statement.Exec(
			span.StartTimeUnixNano,
			span.TraceId,
			span.SpanId,
			span.ParentSpanId,
			span.ServiceName,
			span.Name,
			span.Kind,
			span.DurationNano,
			span.Tags,
			span.TagsKeys,
			span.TagsValues,
			span.StatusCode,
			span.GetReferences(),
			NewNullString(span.ExternalHttpMethod),
			NewNullString(span.ExternalHttpUrl),
			NewNullString(span.Component),
			NewNullString(span.DBSystem),
			NewNullString(span.DBName),
			NewNullString(span.DBOperation),
			NewNullString(span.PeerService),
			span.Events,
			span.HttpUrl,
			span.HttpMethod,
			span.HttpHost,
			span.HttpRoute,
			span.HttpCode,
			span.MsgSystem,
			span.MsgOperation,
			span.HasError,
		)
		if err != nil {
			return err
		}
	}

	commited = true

	return tx.Commit()
}

func (w *SpanWriter) writeErrorBatch(batch []*Span) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}

	commited := false

	defer func() {
		if !commited {
			// Clickhouse does not support real rollback
			_ = tx.Rollback()
		}
	}()

	statement, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s (timestamp, errorID, traceID, spanID, parentSpanID, serviceName, exceptionType, exceptionMessage, excepionStacktrace, exceptionEscaped) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", w.errorTable))
	if err != nil {
		return err
	}

	defer statement.Close()

	for _, span := range batch {
		if span.ErrorEvent.Name == "" {
			continue
		}
		_, err = statement.Exec(
			span.ErrorEvent.TimeUnixNano,
			span.ErrorID,
			span.TraceId,
			span.SpanId,
			span.ParentSpanId,
			span.ServiceName,
			span.ErrorEvent.AttributeMap["exception.type"],
			span.ErrorEvent.AttributeMap["exception.message"],
			span.ErrorEvent.AttributeMap["exception.stacktrace"],
			span.ErrorEvent.AttributeMap["exception.escaped"],
		)
		if err != nil {
			return err
		}
	}

	commited = true

	return tx.Commit()
}

func NewNullString(s string) sql.NullString {
	if len(s) == 0 {
		return sql.NullString{}
	}
	return sql.NullString{
		String: s,
		Valid:  true,
	}
}

// WriteSpan writes the encoded span
func (w *SpanWriter) WriteSpan(span *Span) error {
	w.spans <- span
	return nil
}

// Close Implements io.Closer and closes the underlying storage
func (w *SpanWriter) Close() error {
	w.finish <- true
	w.done.Wait()
	return nil
}
