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
import uuid

import staticconf

from clusterman.interfaces.cluster_connector import ClusterConnector
from clusterman.interfaces.types import AgentMetadata
from clusterman.interfaces.types import AgentState
from clusterman.simulator import simulator
from clusterman.util import ClustermanResources


class SimulatedClusterConnector(ClusterConnector):

    def __init__(self, cluster: str, pool: str, simulator: 'simulator.Simulator') -> None:
        self.cluster = cluster
        self.pool = pool
        self.simulator = simulator

    def reload_state(self) -> None:
        pass

    def get_resource_allocation(self, resource_name: str) -> float:
        return 0

    def get_resource_total(self, resource_name: str) -> float:
        total = 0
        for c in self.simulator.aws_clusters:
            for i in c.instances.values():
                if self.simulator.current_time < i.join_time:
                    continue

                total += getattr(i.resources, resource_name)
        return total

    def _get_agent_metadata(self, instance_ip: str) -> AgentMetadata:
        for c in self.simulator.aws_clusters:
            for i in c.instances.values():
                if instance_ip == i.ip_address:
                    return AgentMetadata(
                        agent_id=str(uuid.uuid4()),
                        state=(
                            AgentState.ORPHANED
                            if self.simulator.current_time < i.join_time
                            else AgentState.IDLE
                        ),
                        total_resources=ClustermanResources(
                            cpus=i.resources.cpus,
                            mem=i.resources.mem * 1000,
                            disk=(i.resources.disk or staticconf.read_int('ebs_volume_size', 0)) * 1000,
                            gpus=(i.resources.gpus),
                        )
                    )

        # if we don't know the given IP then it's orphaned
        return AgentMetadata(state=AgentState.ORPHANED)
