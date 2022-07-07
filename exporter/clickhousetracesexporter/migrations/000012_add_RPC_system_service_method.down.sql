ALTER TABLE signoz_traces.signoz_index_v2
    DROP COLUMN IF EXISTS `rpcSystem`,
    DROP COLUMN IF EXISTS `rpcService`,
    DROP COLUMN IF EXISTS `rpcMethod`,
    DROP COLUMN IF EXISTS `responseStatusCode`;

ALTER TABLE signoz_traces.signoz_index_v2
    DROP INDEX idx_rpcMethod,
    DROP INDEX idx_responseStatusCode;