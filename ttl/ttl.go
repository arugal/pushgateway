// Copyright 2014 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ttl

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/pushgateway/storage"
)

type TimeToLive struct {
	enable   bool
	interval time.Duration
	timeout  time.Duration

	logger      log.Logger
	metricStore storage.MetricStore

	ctx        context.Context
	cancelFunc context.CancelFunc
}

// New return a new TimeToLive. The log.Logger can be nil, in which case no logging is performed.
func New(enable bool, interval, timeout time.Duration, l log.Logger, ms storage.MetricStore) *TimeToLive {
	if l == nil {
		l = log.NewNopLogger()
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &TimeToLive{
		enable:      enable,
		interval:    interval,
		timeout:     timeout,
		logger:      l,
		metricStore: ms,
		ctx:         ctx,
		cancelFunc:  cancelFunc,
	}
}

func (t *TimeToLive) Start() {
	if t.enable {
		level.Info(t.logger).Log("msg", fmt.Sprintf("start time to live. interval: %v timeout: %v", t.interval, t.timeout))
		ticker := time.NewTicker(t.interval)
		for {
			select {
			case <-ticker.C:
				level.Debug(t.logger).Log("msg", "run time to live")
				for _, group := range t.metricStore.GetMetricFamiliesMap() {
					timeout := time.Now().Add(-t.timeout)
					del := true
					for _, metrics := range group.Metrics {
						if metrics.Timestamp.After(timeout) {
							del = false
							break
						}
					}
					if del {
						t.metricStore.SubmitWriteRequest(storage.WriteRequest{
							Labels:    group.Labels,
							Timestamp: time.Now(),
						})
						level.Info(t.logger).Log("msg", "del metric group successful", "labels", group.Labels)
					}
				}
			case <-t.ctx.Done():
				return
			}
		}
	}
}

func (t *TimeToLive) Shutdown() {
	t.cancelFunc()
}
