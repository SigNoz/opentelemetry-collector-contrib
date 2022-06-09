// Copyright The OpenTelemetry Authors
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

package filestorage // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/extension/extensionhelper"
)

// The value of extension "type" in configuration.
const typeStr config.Type = "file_storage"

// NewFactory creates a factory for HostObserver extension.
func NewFactory() component.ExtensionFactory {
	return extensionhelper.NewFactory(
		typeStr,
		createDefaultConfig,
		createExtension)
}

func createDefaultConfig() config.Extension {
	return &Config{
		ExtensionSettings: config.NewExtensionSettings(config.NewComponentID(typeStr)),
		Directory:         getDefaultDirectory(),
		Compaction: &CompactionConfig{
			Directory: getDefaultDirectory(),
			OnStart:   false,
			// use default bbolt value
			// https://github.com/etcd-io/bbolt/blob/d5db64bdbfdee1cb410894605f42ffef898f395d/cmd/bbolt/main.go#L1955
			MaxTransactionSize: 65536,
		},
		Timeout: time.Second,
	}
}

func createExtension(
	_ context.Context,
	params component.ExtensionCreateSettings,
	cfg config.Extension,
) (component.Extension, error) {
	return newLocalFileStorage(params.Logger, cfg.(*Config))
}
