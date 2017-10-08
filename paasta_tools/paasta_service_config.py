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

from service_configuration_lib import read_extra_service_information
from service_configuration_lib import read_service_configuration

from paasta_tools.adhoc_tools import AdhocJobConfig
from paasta_tools.chronos_tools import ChronosJobConfig
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_paasta_branch
from paasta_tools.utils import INSTANCE_TYPES
from paasta_tools.utils import list_clusters
from paasta_tools.utils import load_deployments_json
from paasta_tools.utils import load_v2_deployments_json


log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


class PaastaServiceConfig():
    """PaastaServiceConfig provides useful methods for reading soa-configs and
    iterating instance names or InstanceConfigs objects.

    :Example:

    >>> from paasta_tools.paasta_service_config import PaastaServiceConfig
    >>> from paasta_tools.utils import DEFAULT_SOA_DIR
    >>>
    >>> sc = PaastaServiceConfig(service='fake_service', soa_dir=DEFAULT_SOA_DIR)
    >>>
    >>> for instance in sc.instances(cluster='fake_cluster', instance_type='marathon'):
    ...     print(instance)
    ...
    main
    canary
    >>>
    >>> for instance_config in sc.instance_configs(cluster='fake_cluster', instance_type='marathon'):
    ...     print(instance_config.get_instance())
    ...
    main
    canary
    >>>
    """

    def __init__(self, service: str, soa_dir: str=DEFAULT_SOA_DIR, load_deployments: bool=True):
        self._service = service
        self._soa_dir = soa_dir
        self._load_deployments = load_deployments
        self._clusters = None
        self._general_config = None
        self._deployments_json = None
        self._framework_configs = {}
        self._deployments_json = None

    @property
    def clusters(self):
        """Returns an iterator that yields cluster names for the service.

        :returns: iterator that yields cluster names.
        """
        if self._clusters is None:
            self._clusters = list_clusters(service=self._service, soa_dir=self._soa_dir)
        for cluster in self._clusters:
            yield cluster

    def instances(self, cluster: str, instance_type: str):
        """Returns an iterator that yields instance names as strings.

        :param cluster: The cluster name
        :param instance_type: One of paasta_tools.utils.INSTANCE_TYPES
        :returns: an iterator that yields instance names
        """
        if (cluster, instance_type) not in self._framework_configs:
            self._refresh_framework_config(cluster, instance_type)
        for instance in self._framework_configs.get((cluster, instance_type), []):
            yield instance

    def instance_configs(self, cluster: str, instance_type: str):
        """Returns an iterator that yields InstanceConfig objects.

        :param cluster: The cluster name
        :param instance_type: One of paasta_tools.utils.INSTANCE_TYPES
        :returns: an iterator that yields instances of MarathonServiceConfig, ChronosJobConfig and etc.
        :raises NotImplementedError: when it doesn't know how to create a config for instance_type
        """
        create_config_function = self._get_create_config_function(instance_type)
        if (cluster, instance_type) not in self._framework_configs:
            self._refresh_framework_config(cluster, instance_type)
        for instance, config in self._framework_configs.get((cluster, instance_type), []).items():
            yield create_config_function(cluster, instance, config)

    def instance_config(self, cluster: str, instance: str):
        """Returns an InstanceConfig object for whatever type of instance it is.

        :param cluster: The cluster name
        :param instance: The instance name
        :returns: instance of MarathonServiceConfig, ChronosJobConfig or None
        """
        for instance_type in INSTANCE_TYPES:
            if (cluster, instance_type) not in self._framework_configs:
                self._refresh_framework_config(cluster, instance_type)
            if instance in self._framework_configs[(cluster, instance_type)]:
                create_config_function = self._get_create_config_function(instance_type)
                return create_config_function(
                    cluster=cluster,
                    instance=instance,
                    config=self._framework_configs[(cluster, instance_type)][instance],
                )

    def _get_create_config_function(self, instance_type: str):
        f = {
            'marathon': self._create_marathon_service_config,
            'chronos': self._create_chronos_service_config,
            'adhoc': self._create_adhoc_service_config,
        }.get(instance_type)
        if f is None:
            raise NotImplementedError(
                "instance type %s is not supported by PaastaServiceConfig."
                % instance_type,
            )
        return f

    def _framework_config_filename(self, cluster: str, instance_type: str):
        return "%s-%s" % (instance_type, cluster)

    def _refresh_framework_config(self, cluster: str, instance_type: str):
        conf_name = self._framework_config_filename(cluster, instance_type)
        log.info("Reading configuration file: %s.yaml", conf_name)
        instances = read_extra_service_information(
            service_name=self._service,
            extra_info=conf_name,
            soa_dir=self._soa_dir,
        )
        self._framework_configs[(cluster, instance_type)] = instances

    def _get_branch_dict(self, cluster: str, instance: str, config: dict):
        if self._load_deployments:
            if self._deployments_json is None:
                self._deployments_json = load_deployments_json(self._service, soa_dir=self._soa_dir)
            branch = config.get('branch', get_paasta_branch(cluster, instance))
            return self._deployments_json.get_branch_dict(self._service, branch)
        else:
            return {}

    def _get_branch_dict_v2(self, cluster: str, instance: str, config: dict):
        if self._load_deployments:
            if self._deployments_json is None:
                self._deployments_json = load_v2_deployments_json(self._service, soa_dir=self._soa_dir)
            branch = config.get('branch', get_paasta_branch(cluster, instance))
            deploy_group = config.get('deploy_group', branch)
            return self._deployments_json.get_branch_dict_v2(self._service, branch, deploy_group)
        else:
            return {}

    def _get_merged_config(self, config):
        if self._general_config is None:
            self._general_config = read_service_configuration(
                service_name=self._service,
                soa_dir=self._soa_dir,
            )
        return deep_merge_dictionaries(
            overrides=config,
            defaults=self._general_config,
        )

    def _create_marathon_service_config(self, cluster, instance, config):
        """Create a service instance's configuration for marathon.

        :param cluster: The cluster to read the configuration for
        :param instance: The instance of the service to retrieve
        :param config: the framework instance config.
        :returns: An instance of MarathonServiceConfig
        """
        merged_config = self._get_merged_config(config)
        branch_dict = self._get_branch_dict(cluster, instance, merged_config)

        return MarathonServiceConfig(
            service=self._service,
            cluster=cluster,
            instance=instance,
            config_dict=merged_config,
            branch_dict=branch_dict,
            soa_dir=self._soa_dir,
        )

    def _create_chronos_service_config(self, cluster, instance, config):
        """Create a service instance's configuration for chronos.

        :param cluster: The cluster to read the configuration for
        :param instance: The instance of the service to retrieve
        :param config:
        :returns: An instance of ChronosJobConfig
        """
        merged_config = self._get_merged_config(config)
        branch_dict = self._get_branch_dict(cluster, instance, merged_config)

        return ChronosJobConfig(
            service=self._service,
            cluster=cluster,
            instance=instance,
            config_dict=merged_config,
            branch_dict=branch_dict,
            soa_dir=self._soa_dir,
        )

    def _create_adhoc_service_config(self, cluster, instance, config):
        """Create a service instance's configuration for the adhoc instance type.

        :param cluster: The cluster to read the configuration for
        :param instance: The instance of the service to retrieve
        :param config:
        :returns: An instance of AdhocJobConfig
        """
        merged_config = self._get_merged_config(config)
        branch_dict = self._get_branch_dict_v2(cluster, instance, merged_config)

        return AdhocJobConfig(
            service=self._service,
            cluster=cluster,
            instance=instance,
            config_dict=merged_config,
            branch_dict=branch_dict,
            soa_dir=self._soa_dir,
        )
