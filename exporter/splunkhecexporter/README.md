# Splunk HTTP Event Collector (HEC) Exporter

How to send metrics to a Splunk HEC endpoint.

Supported pipeline types: logs, metrics, traces

> :construction: This receiver is in beta and configuration fields are subject to change.

## Configuration

The following configuration options are required:

- `token` (no default): HEC requires a token to authenticate incoming traffic. To procure a token, please refer to the [Splunk documentation](https://docs.splunk.com/Documentation/Splunk/latest/Data/UsetheHTTPEventCollector).
- `endpoint` (no default): Splunk HEC URL.

The following configuration options can also be configured:

- `source` (no default): Optional Splunk source: https://docs.splunk.com/Splexicon:Source
- `sourcetype` (no default): Optional Splunk source type: https://docs.splunk.com/Splexicon:Sourcetype
- `index` (no default): Splunk index, optional name of the Splunk index targeted
- `max_connections` (default: 100): Maximum HTTP connections to use simultaneously when sending data.
- `disable_compression` (default: false): Whether to disable gzip compression over HTTP.
- `timeout` (default: 10s): HTTP timeout when sending data.
- `insecure_skip_verify` (default: false): Whether to skip checking the certificate of the HEC endpoint when sending data over HTTPS.
- `ca_file` (no default) Path to the CA cert to verify the server being connected to.
- `cert_file` (no default) Path to the TLS cert to use for client connections when TLS client auth is required.
- `key_file` (no default) Path to the TLS key to use for TLS required connections.
- `max_content_length_logs` (default: 2097152): Maximum log data size in bytes per HTTP post limited to 2097152 bytes (2 MiB).
- `max_content_length_metrics` (default: 2097152): Maximum metric data size in bytes per HTTP post limited to 2097152 bytes (2 MiB).
- `splunk_app_name` (default: "OpenTelemetry Collector Contrib") App name is used to track telemetry information for Splunk App's using HEC by App name.
- `splunk_app_version` (default: Current OpenTelemetry Collector Contrib Build Version): App version is used to track telemetry information for Splunk App's using HEC by App version.
- `hec_metadata_to_otel_attrs/source` (default = 'com.splunk.source'): Specifies the mapping of a specific unified model attribute value to the standard source field of a HEC event.
- `hec_metadata_to_otel_attrs/sourcetype` (default = 'com.splunk.sourcetype'): Specifies the mapping of a specific unified model attribute value to the standard sourcetype field of a HEC event.
- `hec_metadata_to_otel_attrs/index` (default = 'com.splunk.index'):  Specifies the mapping of a specific unified model attribute value to the standard index field of a HEC event.
- `hec_metadata_to_otel_attrs/host` (default = 'host.name'):  Specifies the mapping of a specific unified model attribute value to the standard host field and the `host.name` field of a HEC event.
- `otel_to_hec_fields/severity_text` (default = `otel.log.severity.text`): Specifies the name of the field to map the severity text field of log events.
- `otel_to_hec_fields/severity_number` (default = `otel.log.severity.number`): Specifies the name of the field to map the severity number field of log events.
- `otel_to_hec_fields/name` (default = `"otel.log.name`): Specifies the name of the field to map the name field of log events.

In addition, this exporter offers queued retry which is enabled by default.
Information about queued retry configuration parameters can be found
[here](https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/exporterhelper/README.md).
<br />
If you are getting throttled due to high volume of events the collector might experience memory issues, in those cases it is recommended to change the queued retry [configuration](https://github.com/open-telemetry/opentelemetry-collector/tree/main/exporter/exporterhelper#configuration) to drop events more frequently, for example you can reduce the maximum amount of time spent trying to send a batch from 120s (default) to 60s:
```yaml
exporters:
  splunk_hec:
    retry_on_failure:
      max_elapsed_time: 60
```
If that does not resolve the memory issues you can try to reduce it further and adjust the other queued retry parameters accordingly.
<br />
As a last resort after you have tried to solve the memory issues by adjusting the queued retry configuration you can disable it altogether:

```yaml
exporters:
  splunk_hec:
    retry_on_failure:
      enabled: false
```
<br /><br />

Example:

```yaml
exporters:
  splunk_hec:
    # Splunk HTTP Event Collector token.
    token: "00000000-0000-0000-0000-0000000000000"
    # URL to a Splunk instance to send data to.
    endpoint: "https://splunk:8088/services/collector"
    # Optional Splunk source: https://docs.splunk.com/Splexicon:Source
    source: "otel"
    # Optional Splunk source type: https://docs.splunk.com/Splexicon:Sourcetype
    sourcetype: "otel"
    # Splunk index, optional name of the Splunk index targeted.
    index: "metrics"
    # Maximum HTTP connections to use simultaneously when sending data. Defaults to 100.
    max_connections: 200
    # Whether to disable gzip compression over HTTP. Defaults to false.
    disable_compression: false
    # HTTP timeout when sending data. Defaults to 10s.
    timeout: 10s
    tls:
      # Whether to skip checking the certificate of the HEC endpoint when sending data over HTTPS. Defaults to false.
      insecure_skip_verify: false
      # Path to the CA cert to verify the server being connected to.
      ca_file: /certs/ExampleCA.crt
      # Path to the TLS cert to use for client connections when TLS client auth is required.
      cert_file: /certs/HECclient.crt
      # Path to the TLS key to use for TLS required connections.
      key_file: /certs/HECclient.key
    # Application name is used to track telemetry information for Splunk App's using HEC by App name.
    splunk_app_name: "OpenTelemetry-Collector Splunk Exporter"
    # Application version is used to track telemetry information for Splunk App's using HEC by App version.
    splunk_app_version: "v0.0.1"
```

The full list of settings exposed for this exporter are documented [here](config.go)
with detailed sample configurations [here](testdata/config.yaml).

This exporter also offers proxy support as documented
[here](https://github.com/open-telemetry/opentelemetry-collector/tree/main/exporter#proxy-support).
