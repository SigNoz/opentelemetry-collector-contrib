module github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsxrayreceiver

go 1.17

require (
	github.com/aws/aws-sdk-go v1.42.52
	github.com/google/uuid v1.3.0
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/proxy v0.45.1
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/xray v0.45.1
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal v0.45.1
	github.com/stretchr/testify v1.8.0
	go.opentelemetry.io/collector v0.45.0
	go.opentelemetry.io/collector/model v0.45.0
	go.uber.org/zap v1.21.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-logr/logr v1.2.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/knadh/koanf v1.4.0 // indirect
	github.com/magiconair/properties v1.8.5 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/mapstructure v1.4.3 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.6.1 // indirect
	github.com/spf13/cast v1.4.1 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.opentelemetry.io/otel v1.4.0 // indirect
	go.opentelemetry.io/otel/metric v0.27.0 // indirect
	go.opentelemetry.io/otel/trace v1.4.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	golang.org/x/sys v0.0.0-20220114195835-da31bd327af9 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/common v0.45.1
	go.opentelemetry.io/otel/sdk v1.4.0 // indirect
)

replace github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/proxy => ./../../internal/aws/proxy

replace github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/xray => ./../../internal/aws/xray

replace github.com/open-telemetry/opentelemetry-collector-contrib/internal/common => ../../internal/common

replace github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal => ../../internal/coreinternal
