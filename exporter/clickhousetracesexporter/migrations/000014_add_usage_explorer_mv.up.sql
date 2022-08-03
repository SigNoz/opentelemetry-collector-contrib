CREATE TABLE IF NOT EXISTS signoz_traces.usage_explorer (
  hour DateTime64(9) CODEC(DoubleDelta, LZ4),
  service_name LowCardinality(String) CODEC(ZSTD(1)),
  count UInt64 CODEC(T64, ZSTD(1))
) ENGINE SummingMergeTree()
PARTITION BY toDate(hour)
ORDER BY (hour, service_name);

CREATE MATERIALIZED VIEW IF NOT EXISTS signoz_traces.usage_explorer_mv
TO signoz_traces.usage_explorer
AS SELECT
  toStartOfHour(timestamp) as hour,
  serviceName as service_name,
  count() as count
FROM signoz_traces.signoz_index_v2
GROUP BY toStartOfHour(timestamp), serviceName;