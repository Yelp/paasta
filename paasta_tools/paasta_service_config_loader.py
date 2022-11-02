# Copyright 2015-2017 Yelp Inc.
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
from typing import Dict
from typing import Iterable
from typing import List
from typing import Tuple
from typing import Type

from service_configuration_lib import read_service_configuration

from paasta_tools import utils
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import InstanceConfig_T
from paasta_tools.utils import list_clusters
from paasta_tools.utils import load_service_instance_configs
from paasta_tools.utils import load_v2_deployments_json
from paasta_tools.utils import NoDeploymentsAvailable


log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


class PaastaServiceConfigLoader:
    """PaastaServiceConfigLoader provides useful methods for reading soa-configs and
    iterating instance names or InstanceConfigs objects.

    :Example:

    >>> from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
    >>> from paasta_tools.utils import DEFAULT_SOA_DIR
    >>>
    >>> sc = PaastaServiceConfigLoader(service='fake_service', soa_dir=DEFAULT_SOA_DIR)
    >>>
    >>> for instance in sc.instances(cluster='fake_cluster', instance_type_class=MarathonServiceConfig):
    ...     print(instance)
    ...
    main
    canary
    >>>
    >>> for instance_config in sc.instance_configs(cluster='fake_cluster', instance_type_class=MarathonServiceConfig):
    ...     print(instance_config.get_instance())
    ...
    main
    canary
    >>>
    """

    _framework_configs: Dict[Tuple[str, type], Dict[str, utils.InstanceConfigDict]]
    _clusters: List[str]
    _deployments_json: utils.DeploymentsJsonV2

    def __init__(
        self,
        service: str,
        soa_dir: str = DEFAULT_SOA_DIR,
        load_deployments: bool = True,
    ) -> None:
        self._service = service
        self._soa_dir = soa_dir
        self._load_deployments = load_deployments
        self._clusters = None
        self._general_config = None
        self._deployments_json = None
        self._framework_configs = {}

    @property
    def clusters(self) -> Iterable[str]:
        """Returns an iterator that yields cluster names for the service.

        :returns: iterator that yields cluster names.
        """
        if self._clusters is None:
            self._clusters = list_clusters(service=self._service, soa_dir=self._soa_dir)
        for cluster in self._clusters:
            yield cluster

    def instances(
        self, cluster: str, instance_type_class: Type[InstanceConfig_T]
    ) -> Iterable[str]:
        """Returns an iterator that yields instance names as strings.

        :param cluster: The cluster name
        :param instance_type_class: a subclass of InstanceConfig
        :returns: an iterator that yields instance names
        """
        if (cluster, instance_type_class) not in self._framework_configs:
            self._refresh_framework_config(cluster, instance_type_class)
        for instance in self._framework_configs.get((cluster, instance_type_class), []):
            yield instance

    def instance_configs(
        self, cluster: str, instance_type_class: Type[InstanceConfig_T]
    ) -> Iterable[InstanceConfig_T]:
        """Returns an iterator that yields InstanceConfig objects.

        :param cluster: The cluster name
        :param instance_type_class: a subclass of InstanceConfig
        :returns: an iterator that yields instances of MarathonServiceConfig, etc.
        :raises NotImplementedError: when it doesn't know how to create a config for instance_type_class
        """
        if (cluster, instance_type_class) not in self._framework_configs:
            self._refresh_framework_config(cluster, instance_type_class)
        for instance, config in self._framework_configs.get(
            (cluster, instance_type_class), {}
        ).items():
            try:
                yield self._create_service_config(
                    cluster, instance, config, instance_type_class
                )
            except NoDeploymentsAvailable:
                pass

    def _framework_config_filename(
        self, cluster: str, instance_type_class: Type[InstanceConfig_T]
    ):
        return f"{instance_type_class.config_filename_prefix}-{cluster}"

    def _refresh_framework_config(
        self, cluster: str, instance_type_class: Type[InstanceConfig_T]
    ):
        instances = load_service_instance_configs(
            service=self._service,
            instance_type=instance_type_class.config_filename_prefix,
            cluster=cluster,
            soa_dir=self._soa_dir,
        )
        self._framework_configs[(cluster, instance_type_class)] = instances

    def _get_branch_dict(
        self, cluster: str, instance: str, config: utils.InstanceConfig
    ) -> utils.BranchDictV2:
        if self._deployments_json is None:
            self._deployments_json = load_v2_deployments_json(
                self._service, soa_dir=self._soa_dir
            )

        branch = config.get_branch()
        deploy_group = config.get_deploy_group()
        return self._deployments_json.get_branch_dict(
            self._service, branch, deploy_group
        )

    def _get_merged_config(
        self, config: utils.InstanceConfigDict
    ) -> utils.InstanceConfigDict:
        if self._general_config is None:
            self._general_config = read_service_configuration(
                service_name=self._service, soa_dir=self._soa_dir
            )
        return deep_merge_dictionaries(overrides=config, defaults=self._general_config)

    def _create_service_config(
        self,
        cluster: str,
        instance: str,
        config: utils.InstanceConfigDict,
        config_class: Type[InstanceConfig_T],
    ) -> InstanceConfig_T:
        """Create a service instance's configuration for marathon.

        :param cluster: The cluster to read the configuration for
        :param instance: The instance of the service to retrieve
        :param config: the framework instance config.
        :returns: An instance of config_class
        """

        merged_config = self._get_merged_config(config)

        temp_instance_config = config_class(
            service=self._service,
            cluster=cluster,
            instance=instance,
            config_dict=merged_config,
            branch_dict=None,
            soa_dir=self._soa_dir,
        )

        branch_dict = self._get_branch_dict(cluster, instance, temp_instance_config)

        return config_class(
            service=self._service,
            cluster=cluster,
            instance=instance,
            config_dict=merged_config,
            branch_dict=branch_dict,
            soa_dir=self._soa_dir,
        )
