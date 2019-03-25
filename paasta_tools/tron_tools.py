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
import datetime
import difflib
import glob
import json
import os
import pkgutil
import re
import subprocess
from string import Formatter
from typing import List
from typing import Tuple

import service_configuration_lib
import yaml
try:
    from yaml.cyaml import CSafeDumper as Dumper
except ImportError:  # pragma: no cover (no libyaml-dev / pypy)
    Dumper = yaml.SafeDumper

from paasta_tools.tron.client import TronClient
from paasta_tools.tron import tron_command_context
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import InvalidInstanceConfig
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import load_v2_deployments_json
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import paasta_print

from paasta_tools import monitoring_tools
from paasta_tools.monitoring_tools import list_teams
from paasta_tools.utils import get_pipeline_config
from paasta_tools.utils import is_deploy_step
from typing import Optional
from typing import Dict
from typing import Any

MASTER_NAMESPACE = 'MASTER'
SPACER = '.'
VALID_MONITORING_KEYS = set(
    json.loads(
        pkgutil.get_data('paasta_tools.cli', 'schemas/tron_schema.json').decode(),
    )['definitions']['job']['properties']['monitoring']['properties'].keys(),
)


class TronNotConfigured(Exception):
    pass


class InvalidTronConfig(Exception):
    pass


class TronConfig(dict):
    """System-level configuration for Tron."""

    def __init__(self, config):
        super().__init__(config)

    def get_cluster_name(self):
        """:returns The name of the Tron cluster"""
        try:
            return self['cluster_name']
        except KeyError:
            raise TronNotConfigured('Could not find name of Tron cluster in system Tron config')

    def get_url(self):
        """:returns The URL for the Tron master's API"""
        try:
            return self['url']
        except KeyError:
            raise TronNotConfigured('Could not find URL of Tron master in system Tron config')


def get_tronfig_folder(cluster, soa_dir):
    return os.path.join(soa_dir, 'tron', cluster)


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


class StringFormatter(Formatter):
    def __init__(self, context=None):
        Formatter.__init__(self)
        self.context = context

    def get_value(self, key, args, kwds):
        if isinstance(key, str):
            try:
                return kwds[key]
            except KeyError:
                return self.context[key]
            else:
                return Formatter.get_value(key, args, kwds)


def parse_time_variables(
    command: str,
    parse_time: datetime.datetime = None,
) -> str:
    """Parses an input string and uses the Tron-style dateparsing
    to replace time variables. Currently supports only the date/time
    variables listed in the tron documentation:
    http://tron.readthedocs.io/en/latest/command_context.html#built-in-cc

    :param input_string: input string to be parsed
    :param parse_time: Reference Datetime object to parse the date and time strings, defaults to now.
    :returns: A string with the date and time variables replaced
    """
    if parse_time is None:
        parse_time = datetime.datetime.now()
    # We build up a tron context object that has the right
    # methods to parse tron-style time syntax
    job_context = tron_command_context.JobRunContext(tron_command_context.CommandContext())
    # The tron context object needs the run_time attribute set so it knows
    # how to interpret the date strings
    job_context.job_run.run_time = parse_time
    return StringFormatter(job_context).format(command)


class TronActionConfig(InstanceConfig):
    config_filename_prefix = 'tron'

    def __init__(self, service, instance, cluster, config_dict, branch_dict, soa_dir=DEFAULT_SOA_DIR):
        super().__init__(
            cluster=cluster,
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

    def get_deploy_group(self) -> Optional[str]:
        return self.config_dict.get('deploy_group', None)

    def get_cmd(self):
        return self.config_dict.get('command')

    def get_executor(self):
        executor = self.config_dict.get('executor', None)
        return 'mesos' if executor == 'paasta' else executor

    def get_healthcheck_mode(self, _) -> None:
        return None

    def get_node(self):
        return self.config_dict.get('node')

    def get_retries(self):
        return self.config_dict.get('retries')

    def get_retries_delay(self):
        return self.config_dict.get('retries_delay')

    def get_requires(self):
        return self.config_dict.get('requires')

    def get_expected_runtime(self):
        return self.config_dict.get('expected_runtime')

    def get_triggered_by(self):
        return self.config_dict.get('triggered_by', None)

    def get_trigger_downstreams(self):
        return self.config_dict.get('trigger_downstreams', None)

    def get_on_upstream_rerun(self):
        return self.config_dict.get('on_upstream_rerun', None)

    def get_trigger_timeout(self):
        return self.config_dict.get('trigger_timeout', None)

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

    def get_nerve_namespace(self) -> None:
        return None

    def check_deploy_group(self) -> Tuple[bool, str]:
        deploy_group = self.get_deploy_group()
        if deploy_group is not None:
            pipeline_steps = [step['step'] for step in get_pipeline_config(self.service, self.soa_dir)]
            pipeline_deploy_groups = [step for step in pipeline_steps if is_deploy_step(step)]
            if deploy_group not in pipeline_deploy_groups:
                return False, f'deploy_group {deploy_group} is not in service {self.service} deploy.yaml'
        return True, ''

    def validate(self) -> List[str]:
        # Use InstanceConfig to validate shared config keys like cpus and mem
        error_msgs = super().validate()
        name = self.get_instance()
        msgs: List[str] = []
        if error_msgs:
            msgs += [f'{name}: {msg}' for msg in error_msgs]

        check_pass, check_msg = self.check_deploy_group()
        if check_pass is False:
            msgs.append(f'{name}: {check_msg}')

        return msgs


class TronJobConfig:
    """Represents a job in Tron, consisting of action(s) and job-level configuration values."""

    def __init__(
        self, name: str, config_dict: Dict[str, Any], cluster: str, service: Optional[str] = None,
        load_deployments: bool = True, soa_dir: str = DEFAULT_SOA_DIR,
    ) -> None:
        self.name = name
        self.config_dict = config_dict
        self.cluster = cluster
        self.service = service
        self.load_deployments = load_deployments
        self.soa_dir = soa_dir

    def get_name(self):
        return self.name

    def get_node(self):
        return self.config_dict.get('node')

    def get_schedule(self):
        return self.config_dict.get('schedule')

    def get_monitoring(self):
        srv_monitoring = dict(monitoring_tools.read_monitoring_config(
            self.service,
            soa_dir=self.soa_dir,
        ))
        tron_monitoring = self.config_dict.get('monitoring', {})
        srv_monitoring.update(tron_monitoring)
        # filter out non-tron monitoring keys
        srv_monitoring = {
            k: v
            for k, v in srv_monitoring.items()
            if k in VALID_MONITORING_KEYS
        }
        return srv_monitoring

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

    def get_service(self) -> Optional[str]:
        return self.service or self.config_dict.get('service')

    def get_deploy_group(self) -> Optional[str]:
        return self.config_dict.get('deploy_group', None)

    def get_cluster(self):
        return self.cluster

    def get_expected_runtime(self):
        return self.config_dict.get('expected_runtime')

    def _get_action_config(self, action_name, action_dict):
        action_service = action_dict.setdefault('service', self.get_service())
        action_deploy_group = action_dict.setdefault('deploy_group', self.get_deploy_group())
        if action_service and action_deploy_group and self.load_deployments:
            try:
                deployments_json = load_v2_deployments_json(service=action_service, soa_dir=self.soa_dir)
                branch_dict = {
                    'docker_image': deployments_json.get_docker_image_for_deploy_group(action_deploy_group),
                    'git_sha': deployments_json.get_git_sha_for_deploy_group(action_deploy_group),
                    # TODO: add Tron instances when generating deployments json
                    'desired_state': 'start',
                    'force_bounce': None,
                }
            except NoDeploymentsAvailable:
                raise InvalidTronConfig(
                    'No deployment found for action {action} in job {job}, looking for {deploy_group} '
                    'in service {service}'.format(
                        action=action_dict.get('name'),
                        job=self.get_name(),
                        deploy_group=action_deploy_group,
                        service=action_service,
                    ),
                )
        else:
            branch_dict = None
        action_dict['monitoring'] = self.get_monitoring()

        return TronActionConfig(
            service=action_service,
            instance=compose_instance(self.get_name(), action_name),
            cluster=self.get_cluster(),
            config_dict=action_dict,
            branch_dict=branch_dict,
            soa_dir=self.soa_dir,
        )

    def get_actions(self):
        actions = self.config_dict.get('actions')
        if isinstance(actions, list):
            actions = [
                self._get_action_config(
                    action_dict.get('name'),
                    action_dict,
                )
                for action_dict in actions
            ]
        else:
            actions = [
                self._get_action_config(name, action_dict)
                for name, action_dict in actions.items()
            ]

        return actions

    def get_cleanup_action(self):
        action_dict = self.config_dict.get('cleanup_action')
        if not action_dict:
            return None

        # TODO: we should keep this trickery outside paasta repo
        return self._get_action_config('cleanup', action_dict)

    def check_monitoring(self) -> Tuple[bool, str]:
        monitoring = self.get_monitoring()
        valid_teams = list_teams()
        if monitoring is not None:
            team_name = monitoring.get('team', None)
            if team_name is None:
                return False, 'Team name is required for monitoring'
            elif team_name not in valid_teams:
                suggest_teams = difflib.get_close_matches(word=team_name, possibilities=valid_teams)
                return False, f'Invalid team name: {team_name}. Do you mean one of these: {suggest_teams}'
        return True, ''

    def check_actions(self) -> Tuple[bool, List[str]]:
        actions = self.get_actions()
        cleanup_action = self.get_cleanup_action()
        if cleanup_action:
            actions.append(cleanup_action)

        checks_passed = True
        msgs: List[str] = []
        for action in actions:
            action_msgs = action.validate()
            if action_msgs:
                checks_passed = False
                msgs.extend(action_msgs)
        return checks_passed, msgs

    def validate(self) -> List[str]:
        _, error_msgs = self.check_actions()
        checks = ['check_monitoring']
        for check in checks:
            check_passed, check_msg = getattr(self, check)()
            if not check_passed:
                error_msgs.append(check_msg)
        return error_msgs

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.config_dict == other.config_dict
        return False


def format_volumes(paasta_volume_list):
    return [
        {
            'container_path': v['containerPath'],
            'host_path': v['hostPath'],
            'mode': v['mode'],
        } for v in paasta_volume_list
    ]


def format_master_config(master_config, default_volumes, dockercfg_location):
    mesos_options = master_config.get('mesos_options', {})
    mesos_options.update({
        'default_volumes': format_volumes(default_volumes),
        'dockercfg_location': dockercfg_location,
    })
    master_config['mesos_options'] = mesos_options
    return master_config


def format_tron_action_dict(action_config):
    """Generate a dict of tronfig for an action, from the TronActionConfig.

    :param job_config: TronActionConfig
    """
    executor = action_config.get_executor()
    result = {
        'command': action_config.get_cmd(),
        'executor': executor,
        'requires': action_config.get_requires(),
        'node': action_config.get_node(),
        'retries': action_config.get_retries(),
        'retries_delay': action_config.get_retries_delay(),
        'expected_runtime': action_config.get_expected_runtime(),
        'trigger_downstreams': action_config.get_trigger_downstreams(),
        'triggered_by': action_config.get_triggered_by(),
        'on_upstream_rerun': action_config.get_on_upstream_rerun(),
        'trigger_timeout': action_config.get_trigger_timeout(),
    }
    if executor == 'mesos':
        result['cpus'] = action_config.get_cpus()
        result['mem'] = action_config.get_mem()
        result['env'] = action_config.get_env()
        result['extra_volumes'] = format_volumes(action_config.get_extra_volumes())
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

        # If deployments were not loaded
        if not action_config.get_docker_image():
            result['docker_image'] = ''
        else:
            result['docker_image'] = action_config.get_docker_url()

    # Only pass non-None values, so Tron will use defaults for others
    return {key: val for key, val in result.items() if val is not None}


def format_tron_job_dict(job_config):
    """Generate a dict of tronfig for a job, from the TronJobConfig.

    :param job_config: TronJobConfig
    """
    action_dict = {
        action_config.get_action_name(): format_tron_action_dict(action_config)
        for action_config in job_config.get_actions()
    }

    result = {
        'node': job_config.get_node(),
        'schedule': job_config.get_schedule(),
        'actions': action_dict,
        'monitoring': job_config.get_monitoring(),
        'queueing': job_config.get_queueing(),
        'run_limit': job_config.get_run_limit(),
        'all_nodes': job_config.get_all_nodes(),
        'enabled': job_config.get_enabled(),
        'allow_overlap': job_config.get_allow_overlap(),
        'max_runtime': job_config.get_max_runtime(),
        'time_zone': job_config.get_time_zone(),
        'expected_runtime': job_config.get_expected_runtime(),
    }
    cleanup_config = job_config.get_cleanup_action()
    if cleanup_config:
        cleanup_action = format_tron_action_dict(cleanup_config)
        result['cleanup_action'] = cleanup_action

    # Only pass non-None values, so Tron will use defaults for others
    return {key: val for key, val in result.items() if val is not None}


def load_tron_instance_config(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> TronActionConfig:
    jobs, _ = load_tron_service_config(
        service=service,
        cluster=cluster,
        load_deployments=load_deployments,
        soa_dir=soa_dir,
    )
    requested_job, requested_action = instance.split('.')
    for job in jobs:
        if job.get_name() == requested_job:
            for action in job.get_actions():
                if action.get_action_name() == requested_action:
                    return action
    raise NoConfigurationForServiceError(f"No tron configuration found for {service} {instance}")


def load_tron_yaml(service: str, cluster: str, soa_dir: str) -> Dict[str, Any]:
    tronfig_folder = get_tronfig_folder(soa_dir=soa_dir, cluster=cluster)
    config = service_configuration_lib.read_extra_service_information(
        service_name=service,
        extra_info=f'tron-{cluster}',
        soa_dir=soa_dir,
    )
    if not config:
        config = service_configuration_lib._read_yaml_file(os.path.join(tronfig_folder, f"{service}.yaml"))
    if not config:
        raise NoConfigurationForServiceError('No Tron configuration found for service %s' % service)
    return config


def load_tron_service_config(service, cluster, load_deployments=True, soa_dir=DEFAULT_SOA_DIR):
    """Load all configured jobs for a service, and any additional config values."""
    config = load_tron_yaml(service=service, cluster=cluster, soa_dir=soa_dir)
    config = {key: value for key, value in config.items() if not key.startswith('_')}  # filter templates
    extra_config = {key: value for key, value in config.items() if key != 'jobs'}
    jobs = config.get('jobs') or []
    if isinstance(jobs, list):
        job_configs = [
            TronJobConfig(
                name=job.get('name'),
                service=service,
                cluster=cluster,
                config_dict=job,
                load_deployments=load_deployments,
                soa_dir=soa_dir,
            ) for job in jobs
        ]
    else:
        job_configs = [
            TronJobConfig(
                name=name,
                service=service,
                cluster=cluster,
                config_dict=job,
                load_deployments=load_deployments,
                soa_dir=soa_dir,
            ) for name, job in jobs.items()
        ]
    return job_configs, extra_config


def create_complete_config(service, cluster, soa_dir=DEFAULT_SOA_DIR):
    """Generate a namespace configuration file for Tron, for a service."""
    system_paasta_config = load_system_paasta_config()

    job_configs, other_config = load_tron_service_config(
        service=service,
        cluster=cluster,
        load_deployments=True,
        soa_dir=soa_dir,
    )

    if service == MASTER_NAMESPACE:
        other_config = format_master_config(
            other_config,
            system_paasta_config.get_volumes(),
            system_paasta_config.get_dockercfg_location(),
        )

    other_config['jobs'] = {
        job_config.get_name(): format_tron_job_dict(job_config)
        for job_config in job_configs
    }

    return yaml.dump(
        other_config,
        Dumper=Dumper,
        default_flow_style=False,
    )


def validate_complete_config(service: str, cluster: str, soa_dir: str = DEFAULT_SOA_DIR) -> List[str]:
    job_configs, other_config = load_tron_service_config(
        service=service,
        cluster=cluster,
        load_deployments=False,
        soa_dir=soa_dir,
    )

    if service != MASTER_NAMESPACE and other_config:
        other_keys = list(other_config.keys())
        return [
            f'Non-{MASTER_NAMESPACE} namespace cannot have other config values, found {other_keys}',
        ]

    # PaaSTA-specific validation
    for job_config in job_configs:
        check_msgs = job_config.validate()
        if check_msgs:
            return check_msgs

    # Use Tronfig on generated config from PaaSTA to validate the rest
    other_config['jobs'] = {
        job_config.get_name(): format_tron_job_dict(job_config)
        for job_config in job_configs
    }

    complete_config = yaml.dump(other_config, Dumper=Dumper)

    master_config_path = os.path.join(os.path.abspath(soa_dir), 'tron', cluster, MASTER_NAMESPACE + '.yaml')
    proc = subprocess.run(
        ['tronfig', '-', '-V', '-n', service, '-m', master_config_path],
        input=complete_config,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding='utf-8',
    )

    if proc.returncode != 0:
        process_errors = proc.stderr.strip()
        if process_errors:  # Error running tronfig
            paasta_print(proc.stderr)
        return [proc.stdout.strip()]

    return []


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


def list_tron_clusters(service: str, soa_dir: str = DEFAULT_SOA_DIR) -> List[str]:
    """Returns the Tron clusters a service is configured to deploy to."""
    search_re = r'/tron-([0-9a-z-_]*)\.yaml$'
    service_dir = os.path.join(soa_dir, service)
    clusters = []
    for filename in glob.glob(f'{service_dir}/*.yaml'):
        cluster_re_match = re.search(search_re, filename)
        if cluster_re_match is not None:
            clusters.append(cluster_re_match.group(1))
    return clusters


def get_tron_dashboard_for_cluster(cluster: str):
    dashboards = load_system_paasta_config().get_dashboard_links()[cluster]
    if 'Tron' not in dashboards:
        raise Exception(f"tron api endpoint is not defined for cluster {cluster}")
    return dashboards['Tron']
