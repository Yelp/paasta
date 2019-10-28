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


CONFIG_NAMESPACE = 'clusterman_metrics'

CLUSTERMAN_NAME = 'clusterman'

SYSTEM_METRICS = 'system_metrics'  #: metrics collected about the cluster state (e.g., CPU, memory allocation)
APP_METRICS = 'app_metrics'  #: metrics collected from client applications (e.g., number of application runs)
METADATA = 'metadata'  #: metrics collected about the cluster (e.g., current spot prices, instance types present)

METRIC_TYPES = frozenset([
    SYSTEM_METRICS,
    APP_METRICS,
    METADATA,
])
