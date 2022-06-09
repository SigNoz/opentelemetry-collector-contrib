# ClickHouse Exporter

**Status: experimental**

Supported pipeline types: logs.

This exporter supports sending OpenTelemetry logs to [ClickHouse](https://clickhouse.com/). It will also support spans and metrics in the future.
> ClickHouse is an open-source, high performance columnar OLAP database management system for real-time analytics using SQL.
> Throughput can be measured in rows per second or megabytes per second. 
> If the data is placed in the page cache, a query that is not too complex is processed on modern hardware at a speed of approximately 2-10 GB/s of uncompressed data on a single server.
> If 10 bytes of columns are extracted, the speed is expected to be around 100-200 million rows per second.

Note:
Always add [batch-processor](https://github.com/open-telemetry/opentelemetry-collector/tree/main/processor/batchprocessor) to collector pipeline, as [ClickHouse document says:](https://clickhouse.com/docs/en/introduction/performance/#performance-when-inserting-data) 
> We recommend inserting data in packets of at least 1000 rows, or no more than a single request per second. When inserting to a MergeTree table from a tab-separated dump, the insertion speed can be from 50 to 200 MB/s.

## User Cases

1. Use [Grafana Clickhouse datasource](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/) or
[vertamedia-clickhouse-datasource](https://grafana.com/grafana/plugins/vertamedia-clickhouse-datasource/) to make dashboard.
Support time-series graph, table and logs.

2. Analyze logs via powerful clickhouse SQL.

```clickhouse
/* get error count about my service last 1 hour.*/
SELECT count(*)
FROM logs
WHERE SeverityText="ERROR" AND timestamp >= NOW() - INTERVAL 1 HOUR;
/* find error log.*/
SELECT * 
FROM logs 
WHERE SeverityText="ERROR" AND timestamp >= NOW() - INTERVAL 1 HOUR;
/* find log with specific attribute .*/
SELECT Body
FROM logs 
WHERE Attributes.value[indexOf(Attributes.key, 'http.method')] = 'post' AND timestamp >= NOW() - INTERVAL 1 HOUR;
```

## Configuration options

The following settings are required:

- `dsn` (no default): The ClickHouse server DSN (Data Source Name), for example `tcp://127.0.0.1:9000?username=user&password=qwerty&database=default`
   For tcp protocol reference: [ClickHouse/clickhouse-go#dsn](https://github.com/ClickHouse/clickhouse-go#dsn).
   For http protocol reference: [mailru/go-clickhouse/#dsn](https://github.com/mailru/go-clickhouse/#dsn).

The following settings can be optionally configured:

- `ttl_days` (default=0): The data time-to-live in days, 0 means no ttl.
- `timeout` (default = 5s): The timeout for every attempt to send data to the backend.
- `sending_queue`
  - `queue_size` (default = 5000): Maximum number of batches kept in memory before dropping data.
- `retry_on_failure`
    - `enabled` (default = true)
    - `initial_interval` (default = 5s): The Time to wait after the first failure before retrying; ignored if `enabled` is `false`
    - `max_interval` (default = 30s): The upper bound on backoff; ignored if `enabled` is `false`
    - `max_elapsed_time` (default = 300s): The maximum amount of time spent trying to send a batch; ignored if `enabled` is `false`

## Example

```yaml
receivers:
  examplereceiver:
processors:
  batch:
    timeout: 10s
exporters:
  clickhouse:
    dsn: tcp://127.0.0.1:9000?database=default
    ttl_days: 3
    timeout: 5s
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 300s
service:
  pipelines:
    logs:
      receivers: [examplereceiver]
      processors: [batch]
      exporters: [clickhouse]
```

## Schema

```clickhouse
CREATE TABLE IF NOT EXISTS logs (
     timestamp DateTime CODEC(Delta, ZSTD(1)),
     TraceId String CODEC(ZSTD(1)),
     SpanId String CODEC(ZSTD(1)),
     TraceFlags Int64,
     SeverityText LowCardinality(String) CODEC(ZSTD(1)),
     SeverityNumber Int64,
     Name LowCardinality(String) CODEC(ZSTD(1)),
     Body String CODEC(ZSTD(1)),
     Attributes Nested
     (
         key LowCardinality(String),
         value String
     ) CODEC(ZSTD(1)),
     Resource Nested
     (
         key LowCardinality(String),
         value String
     ) CODEC(ZSTD(1)),
     INDEX idx_attr_keys Attributes.key TYPE bloom_filter(0.01) GRANULARITY 64,
     INDEX idx_res_keys Resource.key TYPE bloom_filter(0.01) GRANULARITY 64
) ENGINE MergeTree()
TTL timestamp + INTERVAL 3 DAY
PARTITION BY toDate(timestamp)
ORDER BY (Name, -toUnixTimestamp(timestamp))
```