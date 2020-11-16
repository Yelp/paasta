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
"""
Usage: ./check_cassandracluster_services_replication.py [options]
"""
import logging

from paasta_tools import cassandracluster_tools
from paasta_tools.check_kubernetes_services_replication import (
    check_kubernetes_pod_replication,
)
from paasta_tools.check_services_replication_tools import main


log = logging.getLogger(__name__)


if __name__ == "__main__":
    main(
        cassandracluster_tools.CassandraClusterDeploymentConfig,
        check_kubernetes_pod_replication,
        namespace="paasta-cassandraclusters",
    )
