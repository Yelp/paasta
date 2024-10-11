#!/usr/bin/env python
# Copyright 2015-2019 Yelp Inc.
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
import logging
import time
from collections import namedtuple

from kazoo.exceptions import NoNodeError

from paasta_tools.long_running_service_tools import ZK_PAUSE_AUTOSCALE_PATH
from paasta_tools.utils import ZookeeperPool

ServiceAutoscalingInfo = namedtuple(
    "ServiceAutoscalingInfo",
    [
        "current_instances",
        "max_instances",
        "min_instances",
        "current_utilization",
        "target_instances",
    ],
)


SERVICE_METRICS_PROVIDER_KEY = "metrics_providers"
DECISION_POLICY_KEY = "decision_policy"


log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


def autoscaling_is_paused():
    with ZookeeperPool() as zk:
        try:
            pause_until = zk.get(ZK_PAUSE_AUTOSCALE_PATH)[0].decode("utf8")
            pause_until = float(pause_until)
        except (NoNodeError, ValueError, AttributeError):
            pause_until = 0

    remaining = pause_until - time.time()
    if remaining >= 0:
        log.debug("Autoscaling is paused for {} more seconds".format(str(remaining)))
        return True
    else:
        return False
