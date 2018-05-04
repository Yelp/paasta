import logging
import os
from urllib.parse import urljoin

import requests
import service_configuration_lib
import yaml
try:
    from yaml.cyaml import CSafeDumper as Dumper
except ImportError:  # pragma: no cover (no libyaml-dev / pypy)
    Dumper = yaml.SafeDumper

from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_user_agent
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import InvalidInstanceConfig
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import load_v2_deployments_json


log = logging.getLogger(__name__)

SPACER = '.'


class TronNotConfigured(Exception):
    pass


class TronConfig(dict):

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
    return '%s%s%s' % (job, SPACER, action)


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
        executor = self.config_dict.get('executor', 'ssh')
        return 'mesos' if executor == 'paasta' else executor

    def get_node(self):
        return self.config_dict.get('node')

    def get_retries(self):
        return self.config_dict.get('retries')

    def get_requires(self):
        return self.config_dict.get('requires')

    def get_calculated_constraints(self):
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

    def format_tron_action_dict(self, cluster_fqdn_format):
        executor = self.get_executor()
        action_config = {
            'name': self.get_action_name(),
            'command': self.get_cmd(),
            'executor': executor,
            'requires': self.get_requires(),
            'node': self.get_node(),
            'retries': self.get_retries(),
        }
        if executor == 'mesos':
            action_config['mesos_address'] = cluster_fqdn_format.format(cluster=self.get_cluster())
            action_config['cpus'] = self.get_cpus()
            action_config['mem'] = self.get_mem()
            action_config['docker_image'] = self.get_docker_url()
            action_config['env'] = self.get_env()
            action_config['extra_volumes'] = [
                {
                    'container_path': v['containerPath'],
                    'host_path': v['hostPath'],
                    'mode': v['mode'],
                } for v in self.get_extra_volumes()
            ]
            action_config['docker_parameters'] = [
                {
                    'key': param['key'],
                    'value': param['value'],
                } for param in self.format_docker_parameters()
            ]
            action_config['constraints'] = self.get_calculated_constraints()

        # Only pass non-None values, so Tron will use defaults for others
        return {key: val for key, val in action_config.items() if val is not None}


class TronJobConfig:

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
            deployments_json = load_v2_deployments_json(action_service, soa_dir=self.soa_dir)
            branch_dict = {
                'docker_image': deployments_json.get_docker_image_for_deploy_group(action_deploy_group),
                'git_sha': deployments_json.get_git_sha_for_deploy_group(action_deploy_group),
                # TODO: add Tron instances when generating deployments json
                'desired_state': 'start',
                'force_bounce': None,
            }
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
        return self._get_action_config(action_dict, default_paasta_cluster)

    def format_tron_job_dict(self, cluster_fqdn_format, default_paasta_cluster):
        action_dicts = [
            action_config.format_tron_action_dict(cluster_fqdn_format)
            for action_config in self.get_actions(default_paasta_cluster)
        ]

        job_config = {
            'name': self.get_name(),
            'node': self.get_node(),
            'schedule': self.get_schedule(),
            'actions': action_dicts,
            'monitoring': self.get_monitoring(),
            'queueing': self.get_queueing(),
            'run_limit': self.get_run_limit(),
            'all_nodes': self.get_all_nodes(),
            'enabled': self.get_enabled(),
            'allow_overlap': self.get_allow_overlap(),
            'max_runtime': self.get_max_runtime(),
            'time_zone': self.get_time_zone(),
        }
        cleanup_config = self.get_cleanup_action(default_paasta_cluster)
        if cleanup_config:
            job_config['cleanup_action'] = cleanup_config.format_tron_action_dict(cluster_fqdn_format)

        # Only pass non-None values, so Tron will use defaults for others
        return {key: val for key, val in job_config.items() if val is not None}

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.config_dict == other.config_dict
        return False


def load_tron_service_config(service, tron_cluster, soa_dir=DEFAULT_SOA_DIR):
    tron_conf_file = os.path.join(
        os.path.abspath(soa_dir), 'tron', tron_cluster, service + '.yaml',
    )
    config = service_configuration_lib._read_yaml_file(tron_conf_file)

    extra_config = {key: value for key, value in config.items() if key != 'jobs'}
    job_configs = []
    for job in config.get('jobs', []):
        job_configs.append(TronJobConfig(job, soa_dir))
    return job_configs, extra_config


def create_complete_config(service, soa_dir=DEFAULT_SOA_DIR):
    system_paasta_config = load_system_paasta_config()
    tron_config = load_tron_config()

    job_configs, other_config = load_tron_service_config(
        service=service,
        tron_cluster=tron_config.get_cluster_name(),
        soa_dir=soa_dir,
    )
    other_config['jobs'] = [
        job_config.format_tron_job_dict(
            cluster_fqdn_format=system_paasta_config.get_cluster_fqdn_format(),
            default_paasta_cluster=tron_config.get_default_paasta_cluster(),
        ) for job_config in job_configs
    ]

    return yaml.dump(
        other_config,
        Dumper=Dumper,
        default_flow_style=False,
    )


def get_tron_namespaces_for_cluster(cluster=None, soa_dir=DEFAULT_SOA_DIR):
    if not cluster:
        cluster = load_tron_config().get_cluster_name()

    config_dir = os.path.join(
        os.path.abspath(soa_dir),
        'tron',
        cluster,
    )
    namespaces = [
        os.path.splitext(filename)[0] for filename in os.listdir(config_dir)
    ]
    return namespaces
