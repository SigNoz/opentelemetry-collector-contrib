// Copyright 2020 OpenTelemetry Authors
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

package k8sattributesprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/k8sattributesprocessor"

import (
	"context"
	"fmt"
	"strconv"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/model/pdata"
	conventions "go.opentelemetry.io/collector/model/semconv/v1.8.0"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/k8sconfig"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/k8sattributesprocessor/internal/kube"
)

const (
	k8sIPLabelName    string = "k8s.pod.ip"
	clientIPLabelName string = "ip"
)

type kubernetesprocessor struct {
	logger          *zap.Logger
	apiConfig       k8sconfig.APIConfig
	kc              kube.Client
	passthroughMode bool
	rules           kube.ExtractionRules
	filters         kube.Filters
	podAssociations []kube.Association
	podIgnore       kube.Excludes
}

func (kp *kubernetesprocessor) initKubeClient(logger *zap.Logger, kubeClient kube.ClientProvider) error {
	if kubeClient == nil {
		kubeClient = kube.New
	}
	if !kp.passthroughMode {
		kc, err := kubeClient(logger, kp.apiConfig, kp.rules, kp.filters, kp.podAssociations, kp.podIgnore, nil, nil, nil)
		if err != nil {
			return err
		}
		kp.kc = kc
	}
	return nil
}

func (kp *kubernetesprocessor) Start(_ context.Context, _ component.Host) error {
	if !kp.passthroughMode {
		go kp.kc.Start()
	}
	return nil
}

func (kp *kubernetesprocessor) Shutdown(context.Context) error {
	if !kp.passthroughMode {
		kp.kc.Stop()
	}
	return nil
}

// processTraces process traces and add k8s metadata using resource IP or incoming IP as pod origin.
func (kp *kubernetesprocessor) processTraces(ctx context.Context, td pdata.Traces) (pdata.Traces, error) {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		kp.processResource(ctx, rss.At(i).Resource())
	}

	return td, nil
}

// processMetrics process metrics and add k8s metadata using resource IP, hostname or incoming IP as pod origin.
func (kp *kubernetesprocessor) processMetrics(ctx context.Context, md pdata.Metrics) (pdata.Metrics, error) {
	rm := md.ResourceMetrics()
	for i := 0; i < rm.Len(); i++ {
		kp.processResource(ctx, rm.At(i).Resource())
	}

	return md, nil
}

// processLogs process logs and add k8s metadata using resource IP, hostname or incoming IP as pod origin.
func (kp *kubernetesprocessor) processLogs(ctx context.Context, ld pdata.Logs) (pdata.Logs, error) {
	rl := ld.ResourceLogs()
	for i := 0; i < rl.Len(); i++ {
		kp.processResource(ctx, rl.At(i).Resource())
	}

	return ld, nil
}

// processResource adds Pod metadata tags to resource based on pod association configuration
func (kp *kubernetesprocessor) processResource(ctx context.Context, resource pdata.Resource) {
	podIdentifierKey, podIdentifierValue := extractPodID(ctx, resource.Attributes(), kp.podAssociations)
	if podIdentifierKey != "" {
		resource.Attributes().InsertString(podIdentifierKey, string(podIdentifierValue))
	}

	if kp.passthroughMode {
		return
	}

	if podIdentifierKey != "" {
		if pod, ok := kp.kc.GetPod(podIdentifierValue); ok {
			for key, val := range pod.Attributes {
				resource.Attributes().InsertString(key, val)
			}
			kp.addContainerAttributes(resource.Attributes(), pod)
		}
	}

	namespace := stringAttributeFromMap(resource.Attributes(), conventions.AttributeK8SNamespaceName)
	if namespace != "" {
		attrsToAdd := kp.getAttributesForPodsNamespace(namespace)
		for key, val := range attrsToAdd {
			resource.Attributes().InsertString(key, val)
		}
	}
}

// addContainerAttributes looks if pod has any container identifiers and adds additional container attributes
func (kp *kubernetesprocessor) addContainerAttributes(attrs pdata.AttributeMap, pod *kube.Pod) {
	containerName := stringAttributeFromMap(attrs, conventions.AttributeK8SContainerName)
	if containerName == "" {
		return
	}
	containerSpec, ok := pod.Containers[containerName]
	if !ok {
		return
	}

	if containerSpec.ImageName != "" {
		attrs.InsertString(conventions.AttributeContainerImageName, containerSpec.ImageName)
	}
	if containerSpec.ImageTag != "" {
		attrs.InsertString(conventions.AttributeContainerImageTag, containerSpec.ImageTag)
	}

	runIDAttr, ok := attrs.Get(conventions.AttributeK8SContainerRestartCount)
	if ok {
		runID, err := intFromAttribute(runIDAttr)
		if err == nil {
			if containerStatus, ok := containerSpec.Statuses[runID]; ok && containerStatus.ContainerID != "" {
				attrs.InsertString(conventions.AttributeContainerID, containerStatus.ContainerID)
			}
		} else {
			kp.logger.Debug(err.Error())
		}
	}
}

func (kp *kubernetesprocessor) getAttributesForPodsNamespace(namespace string) map[string]string {
	ns, ok := kp.kc.GetNamespace(namespace)
	if !ok {
		return nil
	}
	return ns.Attributes
}

// intFromAttribute extracts int value from an attribute stored as string or int
func intFromAttribute(val pdata.AttributeValue) (int, error) {
	switch val.Type() {
	case pdata.AttributeValueTypeInt:
		return int(val.IntVal()), nil
	case pdata.AttributeValueTypeString:
		i, err := strconv.Atoi(val.StringVal())
		if err != nil {
			return 0, err
		}
		return i, nil
	default:
		return 0, fmt.Errorf("wrong attribute type %v, expected int", val.Type())
	}
}
