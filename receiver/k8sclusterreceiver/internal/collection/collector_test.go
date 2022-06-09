// Copyright OpenTelemetry Authors
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

package collection

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/maps"
	metadata "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/experimentalmetricmetadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver/internal/testutils"
)

var commonPodMetadata = map[string]string{
	"foo":                    "bar",
	"foo1":                   "",
	"pod.creation_timestamp": "0001-01-01T00:00:00Z",
}

var allPodMetadata = func(metadata map[string]string) map[string]string {
	out := maps.MergeStringMaps(metadata, commonPodMetadata)
	return out
}

func TestDataCollectorSyncMetadata(t *testing.T) {
	tests := []struct {
		name          string
		metadataStore *metadataStore
		resource      interface{}
		want          map[metadata.ResourceID]*KubernetesMetadata
	}{
		{
			name:          "Pod and container metadata simple case",
			metadataStore: &metadataStore{},
			resource: newPodWithContainer(
				"0",
				podSpecWithContainer("container-name"),
				podStatusWithContainer("container-name", "container-id"),
			),
			want: map[metadata.ResourceID]*KubernetesMetadata{
				metadata.ResourceID("test-pod-0-uid"): {
					resourceIDKey: "k8s.pod.uid",
					resourceID:    "test-pod-0-uid",
					metadata:      commonPodMetadata,
				},
				metadata.ResourceID("container-id"): {
					resourceIDKey: "container.id",
					resourceID:    "container-id",
					metadata: map[string]string{
						"container.status": "running",
					},
				},
			},
		},
		{
			name:          "Empty container id skips container resource",
			metadataStore: &metadataStore{},
			resource: newPodWithContainer(
				"0",
				podSpecWithContainer("container-name"),
				podStatusWithContainer("container-name", ""),
			),
			want: map[metadata.ResourceID]*KubernetesMetadata{
				metadata.ResourceID("test-pod-0-uid"): {
					resourceIDKey: "k8s.pod.uid",
					resourceID:    "test-pod-0-uid",
					metadata:      commonPodMetadata,
				},
			},
		},
		{
			name:          "Pod with Owner Reference",
			metadataStore: &metadataStore{},
			resource: withOwnerReferences([]v1.OwnerReference{
				{
					Kind: "StatefulSet",
					Name: "test-statefulset-0",
					UID:  "test-statefulset-0-uid",
				},
			}, newPodWithContainer("0", &corev1.PodSpec{}, &corev1.PodStatus{})),
			want: map[metadata.ResourceID]*KubernetesMetadata{
				metadata.ResourceID("test-pod-0-uid"): {
					resourceIDKey: "k8s.pod.uid",
					resourceID:    "test-pod-0-uid",
					metadata: allPodMetadata(map[string]string{
						"k8s.workload.kind":    "StatefulSet",
						"k8s.workload.name":    "test-statefulset-0",
						"k8s.statefulset.name": "test-statefulset-0",
						"k8s.statefulset.uid":  "test-statefulset-0-uid",
					}),
				},
			},
		},
		{
			name: "Pod with Service metadata",
			metadataStore: &metadataStore{
				services: &testutils.MockStore{
					Cache: map[string]interface{}{
						"test-namespace/test-service": &corev1.Service{
							ObjectMeta: v1.ObjectMeta{
								Name:      "test-service",
								Namespace: "test-namespace",
								UID:       "test-service-uid",
							},
							Spec: corev1.ServiceSpec{
								Selector: map[string]string{
									"k8s-app": "my-app",
								},
							},
						},
					},
				},
			},
			resource: podWithAdditionalLabels(
				map[string]string{"k8s-app": "my-app"},
				newPodWithContainer("0", &corev1.PodSpec{}, &corev1.PodStatus{}),
			),
			want: map[metadata.ResourceID]*KubernetesMetadata{
				metadata.ResourceID("test-pod-0-uid"): {
					resourceIDKey: "k8s.pod.uid",
					resourceID:    "test-pod-0-uid",
					metadata: allPodMetadata(map[string]string{
						"k8s.service.test-service": "",
						"k8s-app":                  "my-app",
					}),
				},
			},
		},
		{
			name:          "Daemonset simple case",
			metadataStore: &metadataStore{},
			resource:      newDaemonset("1"),
			want: map[metadata.ResourceID]*KubernetesMetadata{
				metadata.ResourceID("test-daemonset-1-uid"): {
					resourceIDKey: "k8s.daemonset.uid",
					resourceID:    "test-daemonset-1-uid",
					metadata: map[string]string{
						"k8s.workload.kind":            "DaemonSet",
						"k8s.workload.name":            "test-daemonset-1",
						"daemonset.creation_timestamp": "0001-01-01T00:00:00Z",
					},
				},
			},
		},
		{
			name:          "Deployment simple case",
			metadataStore: &metadataStore{},
			resource:      newDeployment("1"),
			want: map[metadata.ResourceID]*KubernetesMetadata{
				metadata.ResourceID("test-deployment-1-uid"): {
					resourceIDKey: "k8s.deployment.uid",
					resourceID:    "test-deployment-1-uid",
					metadata: map[string]string{
						"k8s.workload.kind":             "Deployment",
						"k8s.workload.name":             "test-deployment-1",
						"k8s.deployment.name":           "test-deployment-1",
						"deployment.creation_timestamp": "0001-01-01T00:00:00Z",
					},
				},
			},
		},
		{
			name:          "HPA simple case",
			metadataStore: &metadataStore{},
			resource:      newHPA("1"),
			want: map[metadata.ResourceID]*KubernetesMetadata{
				metadata.ResourceID("test-hpa-1-uid"): {
					resourceIDKey: "k8s.hpa.uid",
					resourceID:    "test-hpa-1-uid",
					metadata: map[string]string{
						"k8s.workload.kind":      "HPA",
						"k8s.workload.name":      "test-hpa-1",
						"hpa.creation_timestamp": "0001-01-01T00:00:00Z",
					},
				},
			},
		},
		{
			name:          "Job simple case",
			metadataStore: &metadataStore{},
			resource:      newJob("1"),
			want: map[metadata.ResourceID]*KubernetesMetadata{
				metadata.ResourceID("test-job-1-uid"): {
					resourceIDKey: "k8s.job.uid",
					resourceID:    "test-job-1-uid",
					metadata: map[string]string{
						"foo":                    "bar",
						"foo1":                   "",
						"k8s.workload.kind":      "Job",
						"k8s.workload.name":      "test-job-1",
						"job.creation_timestamp": "0001-01-01T00:00:00Z",
					},
				},
			},
		},
		{
			name:          "Node simple case",
			metadataStore: &metadataStore{},
			resource:      newNode("1"),
			want: map[metadata.ResourceID]*KubernetesMetadata{
				metadata.ResourceID("test-node-1-uid"): {
					resourceIDKey: "k8s.node.uid",
					resourceID:    "test-node-1-uid",
					metadata: map[string]string{
						"foo":                     "bar",
						"foo1":                    "",
						"k8s.node.name":           "test-node-1",
						"node.creation_timestamp": "0001-01-01T00:00:00Z",
					},
				},
			},
		},
		{
			name:          "ReplicaSet simple case",
			metadataStore: &metadataStore{},
			resource:      newReplicaSet("1"),
			want: map[metadata.ResourceID]*KubernetesMetadata{
				metadata.ResourceID("test-replicaset-1-uid"): {
					resourceIDKey: "k8s.replicaset.uid",
					resourceID:    "test-replicaset-1-uid",
					metadata: map[string]string{
						"foo":                           "bar",
						"foo1":                          "",
						"k8s.workload.kind":             "ReplicaSet",
						"k8s.workload.name":             "test-replicaset-1",
						"replicaset.creation_timestamp": "0001-01-01T00:00:00Z",
					},
				},
			},
		},
		{
			name:          "ReplicationController simple case",
			metadataStore: &metadataStore{},
			resource: &corev1.ReplicationController{
				ObjectMeta: v1.ObjectMeta{
					Name:        "test-replicationcontroller-1",
					Namespace:   "test-namespace",
					UID:         types.UID("test-replicationcontroller-1-uid"),
					ClusterName: "test-cluster",
				},
			},
			want: map[metadata.ResourceID]*KubernetesMetadata{
				metadata.ResourceID("test-replicationcontroller-1-uid"): {
					resourceIDKey: "k8s.replicationcontroller.uid",
					resourceID:    "test-replicationcontroller-1-uid",
					metadata: map[string]string{
						"k8s.workload.kind":                        "ReplicationController",
						"k8s.workload.name":                        "test-replicationcontroller-1",
						"replicationcontroller.creation_timestamp": "0001-01-01T00:00:00Z",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		observedLogger, _ := observer.New(zapcore.WarnLevel)
		logger := zap.New(observedLogger)
		t.Run(tt.name, func(t *testing.T) {
			dc := &DataCollector{
				logger:                 logger,
				metadataStore:          tt.metadataStore,
				nodeConditionsToReport: []string{},
			}

			actual := dc.SyncMetadata(tt.resource)
			require.Equal(t, len(tt.want), len(actual))

			for key, item := range tt.want {
				got, exists := actual[key]
				require.True(t, exists)
				require.Equal(t, *item, *got)
			}
		})
	}
}
