ALTER TABLE signoz_traces.signoz_index_v2
    ADD COLUMN IF NOT EXISTS `RPCSystem` LowCardinality(String) CODEC(ZSTD(1)),
    ADD COLUMN IF NOT EXISTS `RPCService` LowCardinality(String) CODEC(ZSTD(1)),
    ADD COLUMN IF NOT EXISTS `RPCMethod` LowCardinality(String) CODEC(ZSTD(1))