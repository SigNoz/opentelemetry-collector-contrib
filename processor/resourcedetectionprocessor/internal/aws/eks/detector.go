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

package eks // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourcedetectionprocessor/internal/aws/eks"

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/model/pdata"
	conventions "go.opentelemetry.io/collector/model/semconv/v1.5.0"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/resourcedetectionprocessor/internal"
)

const (
	// TypeStr is type of detector.
	TypeStr = "eks"

	// Environment variable that is set when running on Kubernetes.
	kubernetesServiceHostEnvVar = "KUBERNETES_SERVICE_HOST"
	authConfigmapNS             = "kube-system"
	authConfigmapName           = "aws-auth"
)

type detectorUtils interface {
	getConfigMap(ctx context.Context, namespace string, name string) (map[string]string, error)
}

type eksDetectorUtils struct {
	clientset *kubernetes.Clientset
}

// detector for EKS
type detector struct {
	utils  detectorUtils
	logger *zap.Logger
	err    error
}

var _ internal.Detector = (*detector)(nil)

var _ detectorUtils = (*eksDetectorUtils)(nil)

// NewDetector returns a resource detector that will detect AWS EKS resources.
func NewDetector(set component.ProcessorCreateSettings, _ internal.DetectorConfig) (internal.Detector, error) {
	utils, err := newK8sDetectorUtils()
	return &detector{utils: utils, logger: set.Logger, err: err}, nil
}

// Detect returns a Resource describing the Amazon EKS environment being run in.
func (detector *detector) Detect(ctx context.Context) (resource pdata.Resource, schemaURL string, err error) {
	res := pdata.NewResource()

	//Check if running on EKS.
	isEKS, err := isEKS(ctx, detector.utils)
	if !isEKS {
		detector.logger.Debug("Unable to identify EKS environment", zap.Error(err))
		return res, "", err
	}

	attr := res.Attributes()
	attr.InsertString(conventions.AttributeCloudProvider, conventions.AttributeCloudProviderAWS)
	attr.InsertString(conventions.AttributeCloudPlatform, conventions.AttributeCloudPlatformAWSEKS)

	return res, conventions.SchemaURL, nil
}

func isEKS(ctx context.Context, utils detectorUtils) (bool, error) {
	if os.Getenv(kubernetesServiceHostEnvVar) == "" {
		return false, nil
	}

	// Make HTTP GET request
	awsAuth, err := utils.getConfigMap(ctx, authConfigmapNS, authConfigmapName)
	if err != nil {
		return false, fmt.Errorf("isEks() error retrieving auth configmap: %w", err)
	}

	return awsAuth != nil, nil
}

func newK8sDetectorUtils() (*eksDetectorUtils, error) {
	// Get cluster configuration
	confs, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to create config: %w", err)
	}

	// Create clientset using generated configuration
	clientset, err := kubernetes.NewForConfig(confs)
	if err != nil {
		return nil, fmt.Errorf("failed to create clientset for Kubernetes client")
	}

	return &eksDetectorUtils{clientset: clientset}, nil
}

func (e eksDetectorUtils) getConfigMap(ctx context.Context, namespace string, name string) (map[string]string, error) {
	cm, err := e.clientset.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve ConfigMap %s/%s: %w", namespace, name, err)
	}
	return cm.Data, nil
}
