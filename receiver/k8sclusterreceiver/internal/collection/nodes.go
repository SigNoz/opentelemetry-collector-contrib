// Copyright 2020, OpenTelemetry Authors
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

package collection // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver/internal/collection"

import (
	"fmt"
	"time"

	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	resourcepb "github.com/census-instrumentation/opencensus-proto/gen-go/resource/v1"
	"github.com/iancoleman/strcase"
	conventions "go.opentelemetry.io/collector/model/semconv/v1.5.0"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/maps"
	metadataPkg "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/experimentalmetricmetadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/k8sclusterreceiver/internal/utils"
)

const (
	// Keys for node metadata.
	nodeCreationTime = "node.creation_timestamp"
)

var allocatableDesciption = map[string]string{
	"cpu":               "How many CPU cores remaining that the node can allocate to pods",
	"memory":            "How many bytes of RAM memory remaining that the node can allocate to pods",
	"ephemeral-storage": "How many bytes of ephemeral storage remaining that the node can allocate to pods",
	"storage":           "How many bytes of storage remaining that the node can allocate to pods",
}

func getMetricsForNode(node *corev1.Node, nodeConditionTypesToReport, allocatableTypesToReport []string, logger *zap.Logger) []*resourceMetrics {
	metrics := make([]*metricspb.Metric, 0, len(nodeConditionTypesToReport)+len(allocatableTypesToReport))
	// Adding 'node condition type' metrics
	for _, nodeConditionTypeValue := range nodeConditionTypesToReport {
		nodeConditionMetric := getNodeConditionMetric(nodeConditionTypeValue)
		v1NodeConditionTypeValue := corev1.NodeConditionType(nodeConditionTypeValue)

		metrics = append(metrics, &metricspb.Metric{
			MetricDescriptor: &metricspb.MetricDescriptor{
				Name: nodeConditionMetric,
				Description: fmt.Sprintf("Whether this node is %s (1), "+
					"not %s (0) or in an unknown state (-1)", nodeConditionTypeValue, nodeConditionTypeValue),
				Type: metricspb.MetricDescriptor_GAUGE_INT64,
			},
			Timeseries: []*metricspb.TimeSeries{
				utils.GetInt64TimeSeries(nodeConditionValue(node, v1NodeConditionTypeValue)),
			},
		})
	}
	// Adding 'node allocatable type' metrics
	for _, nodeAllocatableTypeValue := range allocatableTypesToReport {
		nodeAllocatableMetric := getNodeAllocatableMetric(nodeAllocatableTypeValue)
		v1NodeAllocatableTypeValue := corev1.ResourceName(nodeAllocatableTypeValue)
		metricValue, err := nodeAllocatableValue(node, v1NodeAllocatableTypeValue)

		// metrics will be skipped if metric not present in node or value is not convertable to int64
		if err != nil {
			logger.Debug(err.Error())
		} else {
			metrics = append(metrics, &metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        nodeAllocatableMetric,
					Description: allocatableDesciption[v1NodeAllocatableTypeValue.String()],
					Type:        metricspb.MetricDescriptor_GAUGE_INT64,
				},
				Timeseries: []*metricspb.TimeSeries{
					utils.GetInt64TimeSeries(metricValue),
				},
			})
		}
	}

	return []*resourceMetrics{
		{
			resource: getResourceForNode(node),
			metrics:  metrics,
		},
	}
}

func getNodeConditionMetric(nodeConditionTypeValue string) string {
	return fmt.Sprintf("k8s.node.condition_%s", strcase.ToSnake(nodeConditionTypeValue))
}

func getNodeAllocatableMetric(nodeAllocatableTypeValue string) string {
	return fmt.Sprintf("k8s.node.allocatable_%s", strcase.ToSnake(nodeAllocatableTypeValue))
}

func getResourceForNode(node *corev1.Node) *resourcepb.Resource {
	return &resourcepb.Resource{
		Type: k8sType,
		Labels: map[string]string{
			conventions.AttributeK8SNodeUID:     string(node.UID),
			conventions.AttributeK8SNodeName:    node.Name,
			conventions.AttributeK8SClusterName: node.ClusterName,
		},
	}
}

var nodeConditionValues = map[corev1.ConditionStatus]int64{
	corev1.ConditionTrue:    1,
	corev1.ConditionFalse:   0,
	corev1.ConditionUnknown: -1,
}

func nodeAllocatableValue(node *corev1.Node, allocatableType corev1.ResourceName) (int64, error) {
	value, ok := node.Status.Allocatable[allocatableType]
	if !ok {
		return 0, fmt.Errorf("allocatable type %v not found in node %v", allocatableType, node.GetName())
	}

	val, ok := value.AsInt64()
	if !ok {
		return 0, fmt.Errorf("metric %s has value %v which is not convertable to int64", allocatableType, value)
	}
	return val, nil
}

func nodeConditionValue(node *corev1.Node, condType corev1.NodeConditionType) int64 {
	status := corev1.ConditionUnknown
	for _, c := range node.Status.Conditions {
		if c.Type == condType {
			status = c.Status
			break
		}
	}
	return nodeConditionValues[status]
}

func getMetadataForNode(node *corev1.Node) map[metadataPkg.ResourceID]*KubernetesMetadata {
	metadata := maps.MergeStringMaps(map[string]string{}, node.Labels)

	metadata[conventions.AttributeK8SNodeName] = node.Name
	metadata[nodeCreationTime] = node.GetCreationTimestamp().Format(time.RFC3339)

	nodeID := metadataPkg.ResourceID(node.UID)
	return map[metadataPkg.ResourceID]*KubernetesMetadata{
		nodeID: {
			resourceIDKey: conventions.AttributeK8SNodeUID,
			resourceID:    nodeID,
			metadata:      metadata,
		},
	}
}
