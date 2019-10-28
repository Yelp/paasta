# Copyright 2019 Yelp Inc.
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
import staticconf.testing

from clusterman.autoscaler.config import AutoscalingConfig
from clusterman.autoscaler.config import get_autoscaling_config


def test_get_autoscaling_config():
    default_autoscaling_values = {
        'setpoint': 0.7,
        'target_capacity_margin': 0.1,
        'excluded_resources': ['gpus']
    }
    pool_autoscaling_values = {
        'setpoint': 0.8,
        'excluded_resources': ['cpus']
    }
    with staticconf.testing.MockConfiguration({'autoscaling': default_autoscaling_values}), \
            staticconf.testing.MockConfiguration({'autoscaling': pool_autoscaling_values}, namespace='pool_namespace'):
        autoscaling_config = get_autoscaling_config('pool_namespace')

        assert autoscaling_config == AutoscalingConfig(['cpus'], 0.8, 0.1)
