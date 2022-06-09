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

package filestorage

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/extension/experimental/storage"
)

func TestExtensionIntegrity(t *testing.T) {
	ctx := context.Background()
	se := newTestExtension(t)

	type mockComponent struct {
		kind component.Kind
		name config.ComponentID
	}

	components := []mockComponent{
		{kind: component.KindReceiver, name: newTestEntity("receiver_one")},
		{kind: component.KindReceiver, name: newTestEntity("receiver_two")},
		{kind: component.KindProcessor, name: newTestEntity("processor_one")},
		{kind: component.KindProcessor, name: newTestEntity("processor_two")},
		{kind: component.KindExporter, name: newTestEntity("exporter_one")},
		{kind: component.KindExporter, name: newTestEntity("exporter_two")},
		{kind: component.KindExtension, name: newTestEntity("extension_one")},
		{kind: component.KindExtension, name: newTestEntity("extension_two")},
	}

	// Make a client for each component
	clients := make(map[config.ComponentID]storage.Client)
	for _, c := range components {
		client, err := se.GetClient(ctx, c.kind, c.name, "")
		require.NoError(t, err)
		clients[c.name] = client
	}

	thrashClient := func(wg *sync.WaitGroup, n config.ComponentID, c storage.Client) {
		// keys and values
		keys := []string{"a", "b", "c", "d", "e"}
		myBytes := []byte(n.Name())

		// Set my values
		for i := 0; i < len(keys); i++ {
			err := c.Set(ctx, keys[i], myBytes)
			require.NoError(t, err)
		}

		// Repeatedly thrash client
		for j := 0; j < 100; j++ {

			// Make sure my values are still mine
			for i := 0; i < len(keys); i++ {
				v, err := c.Get(ctx, keys[i])
				require.NoError(t, err)
				require.Equal(t, myBytes, v)
			}

			// Delete my values
			for i := 0; i < len(keys); i++ {
				err := c.Delete(ctx, keys[i])
				require.NoError(t, err)
			}

			// Reset my values
			for i := 0; i < len(keys); i++ {
				err := c.Set(ctx, keys[i], myBytes)
				require.NoError(t, err)
			}
		}
		wg.Done()
	}

	// Use clients concurrently
	var wg sync.WaitGroup
	for name, client := range clients {
		wg.Add(1)
		go thrashClient(&wg, name, client)
	}
	wg.Wait()
}

func TestClientHandlesSimpleCases(t *testing.T) {
	ctx := context.Background()
	se := newTestExtension(t)

	client, err := se.GetClient(
		ctx,
		component.KindReceiver,
		newTestEntity("my_component"),
		"",
	)

	myBytes := []byte("value")
	require.NoError(t, err)

	// Set the data
	err = client.Set(ctx, "key", myBytes)
	require.NoError(t, err)

	// Set it again (nop does not error)
	err = client.Set(ctx, "key", myBytes)
	require.NoError(t, err)

	// Get actual data
	data, err := client.Get(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, myBytes, data)

	// Delete the data
	err = client.Delete(ctx, "key")
	require.NoError(t, err)

	// Delete it again (nop does not error)
	err = client.Delete(ctx, "key")
	require.NoError(t, err)

	// Get missing data
	data, err = client.Get(ctx, "key")
	require.NoError(t, err)
	require.Nil(t, data)

}

func TestTwoClientsWithDifferentNames(t *testing.T) {
	ctx := context.Background()
	se := newTestExtension(t)

	client1, err := se.GetClient(
		ctx,
		component.KindReceiver,
		newTestEntity("my_component"),
		"foo",
	)
	require.NoError(t, err)

	client2, err := se.GetClient(
		ctx,
		component.KindReceiver,
		newTestEntity("my_component"),
		"bar",
	)
	require.NoError(t, err)

	myBytes1 := []byte("value1")
	myBytes2 := []byte("value2")

	// Set the data
	err = client1.Set(ctx, "key", myBytes1)
	require.NoError(t, err)

	err = client2.Set(ctx, "key", myBytes2)
	require.NoError(t, err)

	// Check it was associated accordingly
	data, err := client1.Get(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, myBytes1, data)

	data, err = client2.Get(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, myBytes2, data)
}

func TestGetClientErrorsOnDeletedDirectory(t *testing.T) {
	ctx := context.Background()

	tempDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Directory = tempDir

	extension, err := f.CreateExtension(context.Background(), componenttest.NewNopExtensionCreateSettings(), cfg)
	require.NoError(t, err)

	se, ok := extension.(storage.Extension)
	require.True(t, ok)

	// Delete the directory before getting client
	err = os.RemoveAll(tempDir)
	require.NoError(t, err)

	client, err := se.GetClient(
		ctx,
		component.KindReceiver,
		newTestEntity("my_component"),
		"",
	)

	require.Error(t, err)
	require.Nil(t, client)
}

func newTestExtension(t *testing.T) storage.Extension {
	tempDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Directory = tempDir

	extension, err := f.CreateExtension(context.Background(), componenttest.NewNopExtensionCreateSettings(), cfg)
	require.NoError(t, err)

	se, ok := extension.(storage.Extension)
	require.True(t, ok)

	return se
}

func newTestEntity(name string) config.ComponentID {
	return config.NewComponentIDWithName("nop", name)
}

func TestCompaction(t *testing.T) {
	ctx := context.Background()

	tempDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Directory = tempDir

	extension, err := f.CreateExtension(context.Background(), componenttest.NewNopExtensionCreateSettings(), cfg)
	require.NoError(t, err)

	se, ok := extension.(storage.Extension)
	require.True(t, ok)

	client, err := se.GetClient(
		ctx,
		component.KindReceiver,
		newTestEntity("my_component"),
		"",
	)

	require.NoError(t, err)

	files, err := ioutil.ReadDir(tempDir)
	require.NoError(t, err)
	require.Equal(t, 1, len(files))

	file := files[0]
	path := filepath.Join(tempDir, file.Name())
	stats, err := os.Stat(path)
	require.NoError(t, err)
	newStats := stats

	var key string
	i := 0

	// add data until database file changes size (we are checking compacted size)
	for newStats.Size() <= stats.Size() {
		key = fmt.Sprintf("key_%d", i)
		err = client.Set(ctx, key, []byte(key))
		require.NoError(t, err)

		// compact before checking size
		c, fok := client.(*fileStorageClient)
		require.True(t, fok)
		client, err = c.Compact(ctx, tempDir, cfg.Timeout, 1)
		require.NoError(t, err)

		// check size after compaction
		newStats, err = os.Stat(path)
		require.NoError(t, err)
		i++
	}

	stats = newStats

	// compact again just in case
	c, ok := client.(*fileStorageClient)
	require.True(t, ok)
	client, err = c.Compact(ctx, tempDir, cfg.Timeout, 1)
	require.NoError(t, err)

	// check size after compaction
	newStats, err = os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, stats.Size(), newStats.Size())

	// remove data from database
	for i = i - 1; i >= 0; i-- {
		key = fmt.Sprintf("key_%d", i)
		// Set the data
		err = client.Delete(ctx, key)
		require.NoError(t, err)
	}

	// compact after data removal
	c, ok = client.(*fileStorageClient)
	require.True(t, ok)
	_, err = c.Compact(ctx, tempDir, cfg.Timeout, 1)
	require.NoError(t, err)

	// check size
	newStats, err = os.Stat(path)
	require.NoError(t, err)
	require.Less(t, newStats.Size(), stats.Size())
}

// TestCompactionRemoveTemp validates if temporary db used for compaction is removed afterwards
// test is performed for both: the same and different than storage directories
func TestCompactionRemoveTemp(t *testing.T) {
	ctx := context.Background()

	tempDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Directory = tempDir

	extension, err := f.CreateExtension(context.Background(), componenttest.NewNopExtensionCreateSettings(), cfg)
	require.NoError(t, err)

	se, ok := extension.(storage.Extension)
	require.True(t, ok)

	client, err := se.GetClient(
		ctx,
		component.KindReceiver,
		newTestEntity("my_component"),
		"",
	)

	require.NoError(t, err)

	// check if only db exists in tempDir
	files, err := ioutil.ReadDir(tempDir)
	require.NoError(t, err)
	require.Equal(t, 1, len(files))
	fileName := files[0].Name()

	// perform compaction in the same directory
	c, ok := client.(*fileStorageClient)
	require.True(t, ok)
	client, err = c.Compact(ctx, tempDir, cfg.Timeout, 1)
	require.NoError(t, err)

	// check if only db exists in tempDir
	files, err = ioutil.ReadDir(tempDir)
	require.NoError(t, err)
	require.Equal(t, 1, len(files))
	require.Equal(t, fileName, files[0].Name())

	// perform compaction in different directory
	emptyTempDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)

	c, ok = client.(*fileStorageClient)
	require.True(t, ok)
	_, err = c.Compact(ctx, emptyTempDir, cfg.Timeout, 1)
	require.NoError(t, err)

	// check if emptyTempDir is empty after compaction
	files, err = ioutil.ReadDir(emptyTempDir)
	require.NoError(t, err)
	require.Equal(t, 0, len(files))
}
