#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from collections import defaultdict
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import TypedDict


_autoscaling_components: Dict[str, Dict[str, Callable]] = defaultdict(dict)


def register_autoscaling_component(name, method_type):
    def outer(autoscaling_method):
        _autoscaling_components[method_type][name] = autoscaling_method
        return autoscaling_method

    return outer


def get_autoscaling_component(name, method_type):
    return _autoscaling_components[method_type][name]


class MetricsProviderDict(TypedDict, total=False):
    type: str
    decision_policy: str
    setpoint: float
    desired_active_requests_per_replica: int
    forecast_policy: Optional[str]
    moving_average_window_seconds: Optional[int]
    use_resource_metrics: bool
    prometheus_adapter_config: Optional[dict]
    max_instances_alert_threshold: float


class AutoscalingParamsDict(TypedDict, total=False):
    metrics_providers: List[MetricsProviderDict]
    scaledown_policies: Optional[dict]
