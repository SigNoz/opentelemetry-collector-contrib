// Copyright  OpenTelemetry Authors
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

package awscontainerinsightreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver"

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenterror"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"

	ci "github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/containerinsight"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/cadvisor"
	ecsinfo "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/ecsInfo"
	hostInfo "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/host"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/k8sapiserver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscontainerinsightreceiver/internal/stores"
)

var _ component.MetricsReceiver = (*awsContainerInsightReceiver)(nil)

type metricsProvider interface {
	GetMetrics() []pdata.Metrics
}

// awsContainerInsightReceiver implements the component.MetricsReceiver
type awsContainerInsightReceiver struct {
	settings     component.TelemetrySettings
	nextConsumer consumer.Metrics
	config       *Config
	cancel       context.CancelFunc
	cadvisor     metricsProvider
	k8sapiserver metricsProvider
}

// newAWSContainerInsightReceiver creates the aws container insight receiver with the given parameters.
func newAWSContainerInsightReceiver(
	settings component.TelemetrySettings,
	config *Config,
	nextConsumer consumer.Metrics) (component.MetricsReceiver, error) {
	if nextConsumer == nil {
		return nil, componenterror.ErrNilNextConsumer
	}

	r := &awsContainerInsightReceiver{
		settings:     settings,
		nextConsumer: nextConsumer,
		config:       config,
	}
	return r, nil
}

// Start collecting metrics from cadvisor and k8s api server (if it is an elected leader)
func (acir *awsContainerInsightReceiver) Start(ctx context.Context, host component.Host) error {
	ctx, acir.cancel = context.WithCancel(context.Background())

	hostinfo, err := hostInfo.NewInfo(acir.config.ContainerOrchestrator, acir.config.CollectionInterval, acir.settings.Logger)
	if err != nil {
		return err
	}

	if acir.config.ContainerOrchestrator == ci.EKS {
		k8sDecorator, err := stores.NewK8sDecorator(ctx, acir.config.TagService, acir.config.PrefFullPodName, acir.config.AddFullPodNameMetricLabel, acir.settings.Logger)
		if err != nil {
			return err
		}

		decoratorOption := cadvisor.WithDecorator(k8sDecorator)
		acir.cadvisor, err = cadvisor.New(acir.config.ContainerOrchestrator, hostinfo, acir.settings.Logger, decoratorOption)
		if err != nil {
			return err
		}
		acir.k8sapiserver, err = k8sapiserver.New(hostinfo, acir.settings.Logger)
		if err != nil {
			return err
		}
	}
	if acir.config.ContainerOrchestrator == ci.ECS {

		ecsInfo, err := ecsinfo.NewECSInfo(acir.config.CollectionInterval, hostinfo, acir.settings)
		if err != nil {
			return err
		}

		ecsOption := cadvisor.WithECSInfoCreator(ecsInfo)

		acir.cadvisor, err = cadvisor.New(acir.config.ContainerOrchestrator, hostinfo, acir.settings.Logger, ecsOption)
		if err != nil {
			return err
		}
	}

	go func() {
		//cadvisor collects data at dynamical intervals (from 1 to 15 seconds). If the ticker happens
		//at beginning of a minute, it might read the data collected at end of last minute. To avoid this,
		//we want to wait until at least two cadvisor collection intervals happens before collecting the metrics
		secondsInMin := time.Now().Second()
		if secondsInMin < 30 {
			time.Sleep(time.Duration(30-secondsInMin) * time.Second)
		}
		ticker := time.NewTicker(acir.config.CollectionInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				acir.collectData(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

// Shutdown stops the awsContainerInsightReceiver receiver.
func (acir *awsContainerInsightReceiver) Shutdown(context.Context) error {
	acir.cancel()
	return nil
}

// collectData collects container stats from Amazon ECS Task Metadata Endpoint
func (acir *awsContainerInsightReceiver) collectData(ctx context.Context) error {
	var mds []pdata.Metrics
	if acir.cadvisor == nil && acir.k8sapiserver == nil {
		err := errors.New("both cadvisor and k8sapiserver failed to start")
		acir.settings.Logger.Error("Failed to collect stats", zap.Error(err))
		return err
	}

	if acir.cadvisor != nil {
		mds = append(mds, acir.cadvisor.GetMetrics()...)
	}

	if acir.k8sapiserver != nil {
		mds = append(mds, acir.k8sapiserver.GetMetrics()...)
	}

	for _, md := range mds {
		err := acir.nextConsumer.ConsumeMetrics(ctx, md)
		if err != nil {
			return err
		}
	}

	return nil
}
