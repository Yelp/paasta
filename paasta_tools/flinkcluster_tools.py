# Copyright 2015-2019 Yelp Inc.
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
from typing import Any
from typing import List
from typing import Mapping

from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import InstanceConfig


class FlinkClusterConfig(InstanceConfig):
    config_filename_prefix = 'flinkcluster'

    def __init__(self, service, instance, cluster, soa_dir=DEFAULT_SOA_DIR):
        super().__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            soa_dir=soa_dir,
            config_dict={},
            branch_dict={},
        )

    def validate(self) -> List[str]:
        # Use InstanceConfig to validate shared config keys like cpus and mem
        error_msgs = super().validate()

        if error_msgs:
            name = self.get_instance()
            return [f'{name}: {msg}' for msg in error_msgs]
        else:
            return []


def load_flinkcluster_instance_config(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> FlinkClusterConfig:
    return FlinkClusterConfig(
        service=service,
        instance=instance,
        cluster=cluster,
    )


def get_flinkcluster_config(
    kube_client: KubeClient,
    service: str,
    instance: str,
) -> Mapping[str, Any]:
    sanitised_service = service.replace('_', '--')
    sanitised_instance = instance.replace('_', '--')

    group = 'yelp.com'
    version = 'v1alpha1'
    namespace = 'paasta-flinkclusters'
    plural = 'flinkclusters'
    name = f'{sanitised_service}-{sanitised_instance}'
    co = kube_client.custom.get_namespaced_custom_object(
        name=name,
        group=group,
        version=version,
        namespace=namespace,
        plural=plural,
    )
    status = co.get('status')
    return status
