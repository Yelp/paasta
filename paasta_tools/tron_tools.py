# Copyright 2015-2018 Yelp Inc.
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
import os

import service_configuration_lib
import yaml
try:
    from yaml.cyaml import CSafeDumper as Dumper
except ImportError:  # pragma: no cover (no libyaml-dev / pypy)
    Dumper = yaml.SafeDumper

from paasta_tools.tron.client import TronClient
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import InvalidInstanceConfig
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import load_v2_deployments_json
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable

SPACER = '.'


class TronNotConfigured(Exception):
    pass


class InvalidTronConfig(Exception):
    pass


class TronConfig(dict):
    """System-level configuration for Tron."""

    def __init__(self, config):
        super(TronConfig, self).__init__(config)

    def get_cluster_name(self):
        """:returns The name of the Tron cluster"""
        try:
            return self['cluster_name']
        except KeyError:
            raise TronNotConfigured('Could not find name of Tron cluster in system Tron config')

    def get_default_paasta_cluster(self):
        """:returns The PaaSTA cluster to run actions on by default"""
        try:
            return self['default_paasta_cluster']
        except KeyError:
            raise TronNotConfigured('Could not find default PaaSTA cluster in system Tron config')

    def get_url(self):
        """:returns The URL for the Tron master's API"""
        try:
            return self['url']
        except KeyError:
            raise TronNotConfigured('Could not find URL of Tron master in system Tron config')


def load_tron_config():
    return TronConfig(load_system_paasta_config().get_tron_config())


def get_tron_client():
    return TronClient(load_tron_config().get_url())


def compose_instance(job, action):
    return f'{job}{SPACER}{action}'


def decompose_instance(instance):
    """Get (job_name, action_name) from an instance."""
    decomposed = instance.split(SPACER)
    if len(decomposed) != 2:
        raise InvalidInstanceConfig('Invalid instance name: %s' % instance)
    return (decomposed[0], decomposed[1])


class TronActionConfig(InstanceConfig):
    config_filename_prefix = 'tron'

    def __init__(self, service, instance, config_dict, branch_dict, soa_dir=DEFAULT_SOA_DIR):
        super(TronActionConfig, self).__init__(
            cluster=config_dict.get('cluster'),
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
            soa_dir=soa_dir,
        )
        self.job, self.action = decompose_instance(instance)

    def get_job_name(self):
        return self.job

    def get_action_name(self):
        return self.action

    def get_deploy_group(self):
        return self.config_dict.get('deploy_group', '')

    def get_cmd(self):
        return self.config_dict.get('command')

    def get_executor(self):
        executor = self.config_dict.get('executor', None)
        return 'mesos' if executor == 'paasta' else executor

    def get_node(self):
        return self.config_dict.get('node')

    def get_retries(self):
        return self.config_dict.get('retries')

    def get_requires(self):
        return self.config_dict.get('requires')

    def get_calculated_constraints(self):
        """Combine all configured Mesos constraints."""
        constraints = self.get_constraints()
        if constraints is not None:
            return constraints
        else:
            constraints = self.get_extra_constraints()
            constraints.extend(
                self.get_deploy_constraints(
                    blacklist=self.get_deploy_blacklist(),
                    whitelist=self.get_deploy_whitelist(),
                    # Don't have configs for the paasta cluster
                    system_deploy_blacklist=[],
                    system_deploy_whitelist=None,
                ),
            )
            constraints.extend(self.get_pool_constraints())
            return constraints


class TronJobConfig:
    """Represents a job in Tron, consisting of action(s) and job-level configuration values."""

    def __init__(self, config_dict, soa_dir=DEFAULT_SOA_DIR):
        self.config_dict = config_dict
        self.soa_dir = soa_dir
        self._actions = []

    def get_name(self):
        return self.config_dict.get('name')

    def get_node(self):
        return self.config_dict.get('node')

    def get_schedule(self):
        return self.config_dict.get('schedule')

    def get_monitoring(self):
        return self.config_dict.get('monitoring')

    def get_queueing(self):
        return self.config_dict.get('queueing')

    def get_run_limit(self):
        return self.config_dict.get('run_limit')

    def get_all_nodes(self):
        return self.config_dict.get('all_nodes')

    def get_enabled(self):
        return self.config_dict.get('enabled')

    def get_allow_overlap(self):
        return self.config_dict.get('allow_overlap')

    def get_max_runtime(self):
        return self.config_dict.get('max_runtime')

    def get_time_zone(self):
        return self.config_dict.get('time_zone')

    def get_service(self):
        return self.config_dict.get('service')

    def get_deploy_group(self):
        return self.config_dict.get('deploy_group', '')

    def _get_action_config(self, action_dict, default_paasta_cluster):
        action_service = action_dict.setdefault('service', self.get_service())
        action_deploy_group = action_dict.setdefault('deploy_group', self.get_deploy_group())
        if action_service and action_deploy_group:
            try:
                deployments_json = load_v2_deployments_json(action_service, soa_dir=self.soa_dir)
                branch_dict = {
                    'docker_image': deployments_json.get_docker_image_for_deploy_group(action_deploy_group),
                    'git_sha': deployments_json.get_git_sha_for_deploy_group(action_deploy_group),
                    # TODO: add Tron instances when generating deployments json
                    'desired_state': 'start',
                    'force_bounce': None,
                }
            except NoDeploymentsAvailable:
                raise InvalidTronConfig(
                    'No deployment found for action {action} in job {job}, looking for {deploy_group}'
                    'in service {service}'.format(
                        action=action_dict.get('name'),
                        job=self.get_name(),
                        deploy_group=action_deploy_group,
                        service=action_service,
                    ),
                )
        else:
            branch_dict = {}

        if 'cluster' not in action_dict:
            action_dict['cluster'] = default_paasta_cluster

        return TronActionConfig(
            service=action_service,
            instance=compose_instance(self.get_name(), action_dict.get('name')),
            config_dict=action_dict,
            branch_dict=branch_dict,
            soa_dir=self.soa_dir,
        )

    def get_actions(self, default_paasta_cluster):
        if self._actions:
            return self._actions

        actions = []
        for action_dict in self.config_dict.get('actions'):
            actions.append(self._get_action_config(action_dict, default_paasta_cluster))
        self._actions = actions
        return self._actions

    def get_cleanup_action(self, default_paasta_cluster):
        action_dict = self.config_dict.get('cleanup_action')
        if not action_dict:
            return None

        # TODO: we should keep this trickery outside paasta repo
        action_dict['name'] = 'cleanup'
        return self._get_action_config(action_dict, default_paasta_cluster)

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.config_dict == other.config_dict
        return False


def format_tron_action_dict(action_config, cluster_fqdn_format):
    """Generate a dict of tronfig for an action, from the TronActionConfig.

    :param job_config: TronActionConfig
    :param cluster_fqdn_format: format string for the Mesos masters, given the cluster
    """
    executor = action_config.get_executor()
    result = {
        'name': action_config.get_action_name(),
        'command': action_config.get_cmd(),
        'executor': executor,
        'requires': action_config.get_requires(),
        'node': action_config.get_node(),
        'retries': action_config.get_retries(),
    }
    if executor == 'mesos':
        result['mesos_address'] = cluster_fqdn_format.format(cluster=action_config.get_cluster())
        result['cpus'] = action_config.get_cpus()
        result['mem'] = action_config.get_mem()
        result['docker_image'] = action_config.get_docker_url()
        result['env'] = action_config.get_env()
        result['extra_volumes'] = [
            {
                'container_path': v['containerPath'],
                'host_path': v['hostPath'],
                'mode': v['mode'],
            } for v in action_config.get_extra_volumes()
        ]
        result['docker_parameters'] = [
            {
                'key': param['key'],
                'value': param['value'],
            } for param in action_config.format_docker_parameters()
        ]
        constraint_labels = ['attribute', 'operator', 'value']
        result['constraints'] = [
            dict(zip(constraint_labels, constraint))
            for constraint in action_config.get_calculated_constraints()
        ]

    # Only pass non-None values, so Tron will use defaults for others
    return {key: val for key, val in result.items() if val is not None}


def format_tron_job_dict(job_config, cluster_fqdn_format, default_paasta_cluster):
    """Generate a dict of tronfig for a job, from the TronJobConfig.

    :param job_config: TronJobConfig
    :param cluster_fqdn_format: format string for the Mesos masters, given the cluster
    :param default_paasta_cluster: str, PaaSTA cluster to use for each action that
        does not specify a cluster
    """
    action_dicts = [
        format_tron_action_dict(action_config, cluster_fqdn_format)
        for action_config in job_config.get_actions(default_paasta_cluster)
    ]

    result = {
        'name': job_config.get_name(),
        'node': job_config.get_node(),
        'schedule': job_config.get_schedule(),
        'actions': action_dicts,
        'monitoring': job_config.get_monitoring(),
        'queueing': job_config.get_queueing(),
        'run_limit': job_config.get_run_limit(),
        'all_nodes': job_config.get_all_nodes(),
        'enabled': job_config.get_enabled(),
        'allow_overlap': job_config.get_allow_overlap(),
        'max_runtime': job_config.get_max_runtime(),
        'time_zone': job_config.get_time_zone(),
    }
    cleanup_config = job_config.get_cleanup_action(default_paasta_cluster)
    if cleanup_config:
        cleanup_action = format_tron_action_dict(cleanup_config, cluster_fqdn_format)
        del cleanup_action['name']
        result['cleanup_action'] = cleanup_action

    # Only pass non-None values, so Tron will use defaults for others
    return {key: val for key, val in result.items() if val is not None}


def load_tron_service_config(service, tron_cluster, soa_dir=DEFAULT_SOA_DIR):
    """Load all configured jobs for a service, and any additional config values."""

    config = service_configuration_lib.read_extra_service_information(service, 'tron-' + tron_cluster, soa_dir)
    if not config:
        tron_conf_path = os.path.join(
            os.path.abspath(soa_dir), 'tron', tron_cluster, service + '.yaml',
        )
        config = service_configuration_lib._read_yaml_file(tron_conf_path)

    if not config:
        raise NoConfigurationForServiceError('No Tron configuration found for service %s' % service)

    extra_config = {key: value for key, value in config.items() if key != 'jobs'}
    job_configs = [TronJobConfig(job, soa_dir) for job in config.get('jobs') or []]
    return job_configs, extra_config


def create_complete_config(service, soa_dir=DEFAULT_SOA_DIR):
    """Generate a namespace configuration file for Tron, for a service."""
    system_paasta_config = load_system_paasta_config()
    tron_config = load_tron_config()

    job_configs, other_config = load_tron_service_config(
        service=service,
        tron_cluster=tron_config.get_cluster_name(),
        soa_dir=soa_dir,
    )
    other_config['jobs'] = [
        format_tron_job_dict(
            job_config=job_config,
            cluster_fqdn_format=system_paasta_config.get_cluster_fqdn_format(),
            default_paasta_cluster=tron_config.get_default_paasta_cluster(),
        ) for job_config in job_configs
    ]

    return yaml.dump(
        other_config,
        Dumper=Dumper,
        default_flow_style=False,
    )


def _get_tron_namespaces_from_service_dir(cluster, soa_dir):
    tron_config_file = f'tron-{cluster}.yaml'
    config_dirs = [_dir[0] for _dir in os.walk(os.path.abspath(soa_dir)) if tron_config_file in _dir[2]]
    namespaces = [os.path.split(config_dir)[1] for config_dir in config_dirs]
    return namespaces


def _get_tron_namespaces_from_tron_dir(cluster, soa_dir):
    config_dir = os.path.join(
        os.path.abspath(soa_dir),
        'tron',
        cluster,
    )
    namespaces = [
        os.path.splitext(filename)[0] for filename in os.listdir(config_dir)
    ]
    return namespaces


class ConflictingNamespacesError(RuntimeError):
    pass


def get_tron_namespaces_for_cluster(cluster=None, soa_dir=DEFAULT_SOA_DIR):
    """Get all the namespaces that are configured in a particular Tron cluster."""
    if not cluster:
        cluster = load_tron_config().get_cluster_name()

    namespaces1 = set(_get_tron_namespaces_from_service_dir(cluster, soa_dir))
    namespaces2 = set(_get_tron_namespaces_from_tron_dir(cluster, soa_dir))

    if namespaces1.intersection(namespaces2):
        raise ConflictingNamespacesError(
            "namespaces found in both service/*/tron and service/tron/*: {}".
            format(namespaces1.intersection(namespaces2)),
        )

    namespaces = list(namespaces1.union(namespaces2))
    return namespaces
