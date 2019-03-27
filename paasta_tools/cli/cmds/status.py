#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
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
import concurrent.futures
import difflib
import os
import sys
from collections import defaultdict
from datetime import datetime
from distutils.util import strtobool
from itertools import groupby
from typing import Callable
from typing import DefaultDict
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Sequence
from typing import Set
from typing import Tuple
from typing import Type

import humanize
from bravado.exception import HTTPError
from service_configuration_lib import read_deploy

from paasta_tools import kubernetes_tools
from paasta_tools.adhoc_tools import AdhocJobConfig
from paasta_tools.api.client import get_paasta_api_client
from paasta_tools.chronos_tools import ChronosJobConfig
from paasta_tools.cli.utils import execute_paasta_serviceinit_on_remote_master
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import get_instance_configs_for_service
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_deploy_groups
from paasta_tools.flinkcluster_tools import FlinkClusterConfig
from paasta_tools.flinkcluster_tools import get_dashboard_url
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import KubernetesDeployStatus
from paasta_tools.marathon_serviceinit import bouncing_status_human
from paasta_tools.marathon_serviceinit import desired_state_human
from paasta_tools.marathon_serviceinit import marathon_app_deploy_status_human
from paasta_tools.marathon_serviceinit import status_marathon_job_human
from paasta_tools.marathon_tools import MarathonDeployStatus
from paasta_tools.monitoring_tools import get_team
from paasta_tools.monitoring_tools import list_teams
from paasta_tools.tron_tools import TronActionConfig
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import datetime_from_utc_to_local
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_soa_cluster_deploy_files
from paasta_tools.utils import InstanceConfig
from paasta_tools.utils import list_all_instances_for_service
from paasta_tools.utils import list_clusters
from paasta_tools.utils import list_services
from paasta_tools.utils import load_deployments_json
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import SystemPaastaConfig

HTTP_ONLY_INSTANCE_CONFIG: Sequence[Type[InstanceConfig]] = [
    FlinkClusterConfig,
    KubernetesDeploymentConfig,
]
SSH_ONLY_INSTANCE_CONFIG = [
    ChronosJobConfig,
    AdhocJobConfig,
]


def add_subparser(
    subparsers,
) -> None:
    status_parser = subparsers.add_parser(
        'status',
        help="Display the status of a PaaSTA service.",
        description=(
            "'paasta status' works by SSH'ing to remote PaaSTA masters and "
            "inspecting the local APIs, and reports on the overal health "
            "of a service."
        ),
        epilog=(
            "Note: This command requires SSH and sudo privileges on the remote PaaSTA "
            "masters."
        ),
    )
    status_parser.add_argument(
        '-v', '--verbose',
        action='count',
        dest="verbose",
        default=0,
        help="Print out more output regarding the state of the service. "
             "A second -v will also print the stdout/stderr tail.",
    )
    status_parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    add_instance_filter_arguments(status_parser)
    status_parser.set_defaults(command=paasta_status)


def add_instance_filter_arguments(
    status_parser,
    verb: str = 'inspect',
) -> None:
    status_parser.add_argument(
        '-s', '--service',
        help=f'The name of the service you wish to {verb}',
    ).completer = lazy_choices_completer(list_services)
    status_parser.add_argument(
        '-c', '--clusters',
        help=f"A comma-separated list of clusters to {verb}. By default, will {verb} all clusters.\n"
             f"For example: --clusters norcal-prod,nova-prod",
    ).completer = lazy_choices_completer(list_clusters)
    status_parser.add_argument(
        '-i', '--instances',
        help=f"A comma-separated list of instances to {verb}. By default, will {verb} all instances.\n"
             f"For example: --instances canary,main",
    )  # No completer because we need to know service first and we can't until some other stuff has happened
    status_parser.add_argument(
        '-l', '--deploy-group',
        help=(
            f'Name of the deploy group which you want to {verb}. '
            f'If specified together with --instances and/or --clusters, will {verb} common instances only.'
        ),
    ).completer = lazy_choices_completer(list_deploy_groups)
    status_parser.add_argument(
        '-o', '--owner',
        help=f'Only {verb} instances with this owner specified in soa-configs.',
    ).completer = lazy_choices_completer(list_teams)
    status_parser.add_argument(
        '-r', '--registration',
        help=f'Only {verb} instances with this registration.',
    )


def missing_deployments_message(
    service: str,
) -> str:
    message = (
        f"{service} has no deployments in deployments.json yet.\n  "
        "Has Jenkins run?"
    )
    return message


def get_deploy_info(
    deploy_file_path: str,
) -> Mapping:
    deploy_info = read_deploy(deploy_file_path)
    if not deploy_info:
        paasta_print('Error encountered with %s' % deploy_file_path)

        exit(1)
    return deploy_info


def get_planned_deployments(
    service: str,
    soa_dir: str,
) -> Iterable[str]:
    for cluster, cluster_deploy_file in get_soa_cluster_deploy_files(
        service=service,
        soa_dir=soa_dir,
    ):
        for instance in get_deploy_info(cluster_deploy_file):
            yield f'{cluster}.{instance}'


def list_deployed_clusters(
    pipeline: Sequence[str],
    actual_deployments: Sequence[str],
) -> Sequence[str]:
    """Returns a list of clusters that a service is deployed to given
    an input deploy pipeline and the actual deployments"""
    deployed_clusters: List[str] = []
    for namespace in pipeline:
        cluster, instance = namespace.split('.')
        if namespace in actual_deployments:
            if cluster not in deployed_clusters:
                deployed_clusters.append(cluster)
    return deployed_clusters


def get_actual_deployments(
    service: str,
    soa_dir: str,
) -> Mapping[str, str]:
    deployments_json = load_deployments_json(service, soa_dir)
    if not deployments_json:
        paasta_print("Warning: it looks like %s has not been deployed anywhere yet!" % service, file=sys.stderr)
    # Create a dictionary of actual $service Jenkins deployments
    actual_deployments = {}
    for key, branch_dict in deployments_json.config_dict.items():
        service, namespace = key.split(':')
        if service == service:
            value = branch_dict['docker_image']
            sha = value[value.rfind('-') + 1:]
            actual_deployments[namespace.replace('paasta-', '', 1)] = sha
    return actual_deployments


def paasta_status_on_api_endpoint(
    cluster: str,
    service: str,
    instance: str,
    output: List[str],
    system_paasta_config: SystemPaastaConfig,
    verbose: int,
) -> int:
    client = get_paasta_api_client(cluster, system_paasta_config)
    if not client:
        paasta_print('Cannot get a paasta-api client')
        exit(1)

    try:
        status = client.service.status_instance(service=service, instance=instance).result()
    except HTTPError as exc:
        paasta_print(exc.response.text)
        return exc.status_code

    output.append('    instance: %s' % PaastaColors.blue(instance))
    if status.git_sha != '':
        output.append('    Git sha:    %s (desired)' % status.git_sha)

    if status.marathon is not None:
        return print_marathon_status(service, instance, output, status.marathon)
    elif status.kubernetes is not None:
        return print_kubernetes_status(service, instance, output, status.kubernetes)
    elif status.tron is not None:
        return print_tron_status(service, instance, output, status.tron, verbose)
    elif status.flinkcluster is not None:
        return print_flinkcluster_status(cluster, service, instance, output, status.flinkcluster.get('status'), verbose)
    else:
        paasta_print("Not implemented: Looks like %s is not a Marathon or Kubernetes instance" % instance)
        return 0


def print_marathon_status(
    service: str,
    instance: str,
    output: List[str],
    marathon_status,
) -> int:
    if marathon_status.error_message:
        output.append(marathon_status.error_message)
        return 1

    bouncing_status = bouncing_status_human(
        marathon_status.app_count,
        marathon_status.bounce_method,
    )
    desired_state = desired_state_human(
        marathon_status.desired_state,
        marathon_status.expected_instance_count,
    )
    output.append(f"    State:      {bouncing_status} - Desired state: {desired_state}")

    status = MarathonDeployStatus.fromstring(marathon_status.deploy_status)
    if status != MarathonDeployStatus.NotRunning:
        if status == MarathonDeployStatus.Delayed:
            deploy_status = marathon_app_deploy_status_human(status, marathon_status.backoff_seconds)
        else:
            deploy_status = marathon_app_deploy_status_human(status)
    else:
        deploy_status = 'NotRunning'

    output.append(
        "    {}".format(
            status_marathon_job_human(
                service=service,
                instance=instance,
                deploy_status=deploy_status,
                desired_app_id=marathon_status.app_id,
                app_count=marathon_status.app_count,
                running_instances=marathon_status.running_instance_count,
                normal_instance_count=marathon_status.expected_instance_count,
            ),
        ),
    )
    return 0


def kubernetes_app_deploy_status_human(status, backoff_seconds=None):
    status_string = kubernetes_tools.KubernetesDeployStatus.tostring(status)

    if status == kubernetes_tools.KubernetesDeployStatus.Waiting:
        deploy_status = "%s (new tasks waiting for capacity to become available)" % PaastaColors.red(status_string)
    elif status == kubernetes_tools.KubernetesDeployStatus.Deploying:
        deploy_status = PaastaColors.yellow(status_string)
    elif status == kubernetes_tools.KubernetesDeployStatus.Running:
        deploy_status = PaastaColors.bold(status_string)
    else:
        deploy_status = status_string

    return deploy_status


def status_kubernetes_job_human(
    service: str,
    instance: str,
    deploy_status: str,
    desired_app_id: str,
    app_count: int,
    running_instances: int,
    normal_instance_count: int,
) -> str:
    name = PaastaColors.cyan(compose_job_id(service, instance))

    if app_count >= 0:
        if running_instances >= normal_instance_count:
            status = PaastaColors.green("Healthy")
            instance_count = PaastaColors.green("(%d/%d)" % (running_instances, normal_instance_count))
        elif running_instances == 0:
            status = PaastaColors.yellow("Critical")
            instance_count = PaastaColors.red("(%d/%d)" % (running_instances, normal_instance_count))
        else:
            status = PaastaColors.yellow("Warning")
            instance_count = PaastaColors.yellow("(%d/%d)" % (running_instances, normal_instance_count))
        return "Kubernetes:   {} - up with {} instances. Status: {}".format(
            status, instance_count, deploy_status,
        )
    else:
        status = PaastaColors.yellow("Warning")
        return "Kubernetes:   {} - {} (app {}) is not configured in Kubernetes yet (waiting for bounce)".format(
            status, name, desired_app_id,
        )


def print_flinkcluster_status(
    cluster: str,
    service: str,
    instance: str,
    output: List[str],
    status,
    verbose: int,
) -> int:
    if status is None:
        output.append(PaastaColors.red("    Flink cluster is not available yet"))
        return 1

    if status.state != "running":
        output.append("    State: {state}".format(
            state=PaastaColors.yellow(status.state),
        ))
        output.append(f"    No other information available in non-running state")
        return 0

    dashboard_url = get_dashboard_url(
        cluster=cluster,
        service=service,
        instance=instance,
    )
    if verbose:
        output.append(f"    Flink version: {status.config['flink-version']} {status.config['flink-revision']}")
    else:
        output.append(f"    Flink version: {status.config['flink-version']}")
    output.append(f"    URL: {dashboard_url}/")
    output.append(f"    State: {status.state}")
    output.append(
        "    Jobs:"
        f" {status.overview['jobs-running']} running,"
        f" {status.overview['jobs-finished']} finished,"
        f" {status.overview['jobs-failed']} failed,"
        f" {status.overview['jobs-cancelled']} cancelled",
    )
    output.append(
        "   "
        f" {status.overview['taskmanagers']} taskmanagers,"
        f" {status.overview['slots-available']}/{status.overview['slots-total']} slots available",
    )

    output.append(f"    Jobs:")
    if verbose:
        output.append(f"      Job Name                         State       Job ID                           Started")
    else:
        output.append(f"      Job Name                         State       Started")
    # Use only the most recent jobs
    unique_jobs = (
        sorted(jobs, key=lambda j: -j['start-time'])[0]
        for _, jobs in groupby(
            sorted(status.jobs, key=lambda j: j['name']),
            lambda j: j['name'],
        )
    )
    for job in unique_jobs:
        job_id = job['jid']
        if verbose:
            fmt = """      {job_name: <32.32} {state: <11} {job_id} {start_time}
        {dashboard_url}"""
        else:
            fmt = "      {job_name: <32.32} {state: <11} {start_time}"
        start_time = datetime_from_utc_to_local(datetime.utcfromtimestamp(int(job['start-time']) // 1000))
        output.append(fmt.format(
            job_id=job_id,
            job_name=job['name'].split('.', 2)[2],
            state=job['state'],
            start_time=f'{str(start_time)} ({humanize.naturaltime(start_time)})',
            dashboard_url=PaastaColors.grey(
                f'{dashboard_url}/#/jobs/{job_id}',
            ),
        ))
        if job_id in status.exceptions:
            exceptions = status.exceptions[job_id]
            root_exception = exceptions['root-exception']
            if root_exception is not None:
                output.append(f"        Exception: {root_exception}")
                ts = exceptions['timestamp']
                if ts is not None:
                    exc_ts = datetime_from_utc_to_local(datetime.utcfromtimestamp(int(ts) // 1000))
                    output.append(f"            {str(exc_ts)} ({humanize.naturaltime(exc_ts)})")
    return 0


def print_kubernetes_status(
    service: str,
    instance: str,
    output: List[str],
    kubernetes_status,
) -> int:
    if kubernetes_status.error_message:
        output.append(kubernetes_status.error_message)
        return 1

    bouncing_status = bouncing_status_human(
        kubernetes_status.app_count,
        kubernetes_status.bounce_method,
    )
    desired_state = desired_state_human(
        kubernetes_status.desired_state,
        kubernetes_status.expected_instance_count,
    )
    output.append(f"    State:      {bouncing_status} - Desired state: {desired_state}")

    status = KubernetesDeployStatus.fromstring(kubernetes_status.deploy_status)
    deploy_status = kubernetes_app_deploy_status_human(status)

    output.append(
        "    {}".format(
            status_kubernetes_job_human(
                service=service,
                instance=instance,
                deploy_status=deploy_status,
                desired_app_id=kubernetes_status.app_id,
                app_count=kubernetes_status.app_count,
                running_instances=kubernetes_status.running_instance_count,
                normal_instance_count=kubernetes_status.expected_instance_count,
            ),
        ),
    )
    return 0


def print_tron_status(
    service: str,
    instance: str,
    output: List[str],
    tron_status,
    verbose: int = 0,
) -> int:
    output.append(f"    Tron job: {tron_status.job_name}")
    if verbose:
        output.append(f"      Status: {tron_status.job_status}")
        output.append(f"      Schedule: {tron_status.job_schedule}")
    output.append("      Dashboard: {}".format(PaastaColors.blue(tron_status.job_url)))

    output.append(f"    Action: {tron_status.action_name}")
    output.append(f"      Status: {tron_status.action_state}")
    if verbose:
        output.append(f"      Start time: {tron_status.action_start_time}")
    output.append(f"      Command: {tron_status.action_command}")
    if verbose > 1:
        output.append(f"      Raw Command: {tron_status.action_raw_command}")
        output.append(f"      Stdout: \n{tron_status.action_stdout}")
        output.append(f"      Stderr: \n{tron_status.action_stderr}")

    return 0


def report_status_for_cluster(
    service: str,
    cluster: str,
    deploy_pipeline: Sequence[str],
    actual_deployments: Mapping[str, str],
    instance_whitelist: Mapping[str, Type[InstanceConfig]],
    system_paasta_config: SystemPaastaConfig,
    verbose: int = 0,
    use_api_endpoint: bool = False,
) -> Tuple[int, Sequence[str]]:
    """With a given service and cluster, prints the status of the instances
    in that cluster"""
    output = ['', 'service: %s' % service, 'cluster: %s' % cluster]
    seen_instances = []
    deployed_instances = []
    instances = instance_whitelist.keys()
    http_only_instances = [
        instance for instance, instance_config_class in instance_whitelist.items() if instance_config_class
        in HTTP_ONLY_INSTANCE_CONFIG
    ]
    ssh_only_instances = [
        instance for instance, instance_config_class in instance_whitelist.items() if instance_config_class
        in SSH_ONLY_INSTANCE_CONFIG
    ]

    tron_jobs = [
        instance for instance, instance_config_class in instance_whitelist.items() if instance_config_class
        == TronActionConfig
    ]

    for namespace in deploy_pipeline:
        cluster_in_pipeline, instance = namespace.split('.')
        seen_instances.append(instance)

        if cluster_in_pipeline != cluster:
            continue
        if instances and instance not in instances:
            continue

        # Case: service deployed to cluster.instance
        if namespace in actual_deployments:
            deployed_instances.append(instance)

        # Case: flinkcluster instances don't use `deployments.json`
        elif instance_whitelist.get(instance) == FlinkClusterConfig:
            deployed_instances.append(instance)

        # Case: service NOT deployed to cluster.instance
        else:
            output.append('  instance: %s' % PaastaColors.red(instance))
            output.append('    Git sha:    None (not deployed yet)')

    api_return_code = 0
    ssh_return_code = 0
    if len(deployed_instances) > 0:
        http_only_deployed_instances = [
            deployed_instance
            for deployed_instance in deployed_instances
            if (
                deployed_instance in http_only_instances
                or deployed_instance not in ssh_only_instances and use_api_endpoint
            )
        ]
        if len(http_only_deployed_instances):
            return_codes = [
                paasta_status_on_api_endpoint(
                    cluster=cluster,
                    service=service,
                    instance=deployed_instance,
                    output=output,
                    system_paasta_config=system_paasta_config,
                    verbose=verbose,
                )
                for deployed_instance in http_only_deployed_instances
            ]
            if any(return_codes):
                api_return_code = 1
        ssh_only_deployed_instances = [
            deployed_instance
            for deployed_instance in deployed_instances
            if (
                deployed_instance in ssh_only_instances
                or deployed_instance not in http_only_instances and not use_api_endpoint
            )
        ]
        if len(ssh_only_deployed_instances):
            ssh_return_code, status = execute_paasta_serviceinit_on_remote_master(
                'status', cluster, service, ','.join(
                    deployed_instance
                    for deployed_instance in ssh_only_deployed_instances
                ),
                system_paasta_config, stream=False, verbose=verbose,
                ignore_ssh_output=True,
            )
            # Status results are streamed. This print is for possible error messages.
            if status is not None:
                for line in status.rstrip().split('\n'):
                    output.append('    %s' % line)

    if len(tron_jobs) > 0:
        return_codes = [
            paasta_status_on_api_endpoint(
                cluster=cluster,
                service=service,
                instance=tron_job,
                output=output,
                system_paasta_config=system_paasta_config,
                verbose=verbose,
            )
            for tron_job in tron_jobs
        ]
        seen_instances.extend(tron_jobs)

    output.append(report_invalid_whitelist_values(instances, seen_instances, 'instance'))

    if ssh_return_code:
        return_code = ssh_return_code
    elif api_return_code:
        return_code = api_return_code
    else:
        return_code = 0

    return return_code, output


def report_invalid_whitelist_values(
    whitelist: Iterable[str],
    items: Sequence[str],
    item_type: str,
) -> str:
    """Warns the user if there are entries in ``whitelist`` which don't
    correspond to any item in ``items``. Helps highlight typos.
    """
    return_string = ""
    bogus_entries = []
    if whitelist is None:
        return ''
    for entry in whitelist:
        if entry not in items:
            bogus_entries.append(entry)
    if len(bogus_entries) > 0:
        return_string = (
            "\n"
            "Warning: This service does not have any %s matching these names:\n%s"
        ) % (item_type, ",".join(bogus_entries))
    return return_string


def verify_instances(
    args_instances: str,
    service: str,
    clusters: Sequence[str],
) -> Sequence[str]:
    """Verify that a list of instances specified by user is correct for this service.

    :param args_instances: a list of instances.
    :param service: the service name
    :param cluster: a list of clusters
    :returns: a list of instances specified in args_instances without any exclusions.
    """
    unverified_instances = args_instances.split(",")
    service_instances: Set[str] = list_all_instances_for_service(service, clusters=clusters)

    misspelled_instances: Sequence[str] = [i for i in unverified_instances if i not in service_instances]

    if misspelled_instances:
        suggestions: List[str] = []
        for instance in misspelled_instances:
            suggestions.extend(difflib.get_close_matches(instance, service_instances, n=5, cutoff=0.5))  # type: ignore
        suggestions = list(set(suggestions))

        if clusters:
            message = (
                "%s doesn't have any instances matching %s on %s."
                % (
                    service,
                    ', '.join(sorted(misspelled_instances)),
                    ', '.join(sorted(clusters)),
                )
            )
        else:
            message = ("%s doesn't have any instances matching %s."
                       % (service, ', '.join(sorted(misspelled_instances))))

        paasta_print(PaastaColors.red(message))

        if suggestions:
            paasta_print("Did you mean any of these?")
            for instance in sorted(suggestions):
                paasta_print("  %s" % instance)

    return unverified_instances


def normalize_registrations(
    service: str,
    registrations: Sequence[str],
) -> Sequence[str]:
    ret = []
    for reg in registrations:
        if '.' not in reg:
            ret.append(f"{service}.{reg}")
        else:
            ret.append(reg)
    return ret


def get_filters(
    args,
) -> Sequence[Callable[[InstanceConfig], bool]]:
    """Figures out which filters to apply from an args object, and returns them

    :param args: args object
    :returns: list of functions that take an instance config and returns if the instance conf matches the filter
    """
    filters = []

    if args.service:
        filters.append(lambda conf: conf.get_service() in args.service.split(','))

    if args.clusters:
        filters.append(lambda conf: conf.get_cluster() in args.clusters.split(','))

    if args.instances:
        filters.append(lambda conf: conf.get_instance() in args.instances.split(','))

    if args.deploy_group:
        filters.append(lambda conf: conf.get_deploy_group() in args.deploy_group.split(','))

    if args.registration:
        normalized_regs = normalize_registrations(
            service=args.service,
            registrations=args.registration.split(','),
        )
        filters.append(
            lambda conf: any(
                reg in normalized_regs
                for reg in (conf.get_registrations() if hasattr(conf, 'get_registrations') else [])
            ),
        )

    if args.owner:
        owners = args.owner.split(',')

        filters.append(
            # If the instance owner is None, check the service owner, else check the instance owner
            lambda conf: get_team(
                overrides={},
                service=conf.get_service(),
                soa_dir=args.soa_dir,
            ) in owners if conf.get_team() is None else conf.get_team() in owners,
        )

    return filters


def apply_args_filters(
    args,
) -> Mapping[str, Mapping[str, Mapping[str, Type[InstanceConfig]]]]:
    """
    Take an args object and returns the dict of cluster:service:instances
    Currently, will filter by clusters, instances, services, and deploy_groups
    If no instances are found, will print a message and try to find matching instances
    for each service

    :param args: args object containing attributes to filter by
    :returns: Dict of dicts, in format {cluster_name: {service_name: {instance1, instance2}}}
    """
    clusters_services_instances: DefaultDict[
        str,
        DefaultDict[
            str, Dict[str, Type[InstanceConfig]]
        ]
    ] = defaultdict(lambda: defaultdict(dict))

    if args.service is None and args.owner is None:
        args.service = figure_out_service_name(args, soa_dir=args.soa_dir)

    filters = get_filters(args)

    all_services = list_services(soa_dir=args.soa_dir)

    if args.service and args.service not in all_services:
        paasta_print(PaastaColors.red(f'The service "{args.service}" does not exist.'))
        suggestions = difflib.get_close_matches(args.service, all_services, n=5, cutoff=0.5)
        if suggestions:
            paasta_print(PaastaColors.red(f'Did you mean any of these?'))
            for suggestion in suggestions:
                paasta_print(PaastaColors.red(f'  {suggestion}'))
        return clusters_services_instances

    i_count = 0
    for service in all_services:
        if args.service and service != args.service:
            continue

        for instance_conf in get_instance_configs_for_service(service, soa_dir=args.soa_dir):
            if all([f(instance_conf) for f in filters]):
                cluster_service = clusters_services_instances[instance_conf.get_cluster()][service]
                cluster_service[instance_conf.get_instance()] = instance_conf.__class__
                i_count += 1

    if i_count == 0 and args.service and args.instances:
        if args.clusters:
            clusters = args.clusters.split(',')
        else:
            clusters = list_clusters()
        for service in args.service.split(','):
            verify_instances(args.instances, service, clusters)

    return clusters_services_instances


def paasta_status(
    args,
) -> int:
    """Print the status of a Yelp service running on PaaSTA.
    :param args: argparse.Namespace obj created from sys.args by cli"""
    soa_dir = args.soa_dir
    system_paasta_config = load_system_paasta_config()

    if 'USE_API_ENDPOINT' in os.environ:
        use_api_endpoint = strtobool(os.environ['USE_API_ENDPOINT'])
    else:
        use_api_endpoint = False

    return_codes = [0]
    tasks = []
    clusters_services_instances = apply_args_filters(args)
    for cluster, service_instances in clusters_services_instances.items():
        for service, instances in service_instances.items():
            all_flink = all(i == FlinkClusterConfig for i in instances.values())
            actual_deployments: Mapping[str, str]
            if all_flink:
                actual_deployments = {}
            else:
                actual_deployments = get_actual_deployments(service, soa_dir)
            if all_flink or actual_deployments:
                deploy_pipeline = list(get_planned_deployments(service, soa_dir))
                tasks.append((
                    report_status_for_cluster, dict(
                        service=service,
                        cluster=cluster,
                        deploy_pipeline=deploy_pipeline,
                        actual_deployments=actual_deployments,
                        instance_whitelist=instances,
                        system_paasta_config=system_paasta_config,
                        verbose=args.verbose,
                        use_api_endpoint=use_api_endpoint,
                    ),
                ))
            else:
                paasta_print(missing_deployments_message(service))
                return_codes.append(1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        tasks = [executor.submit(t[0], **t[1]) for t in tasks]  # type: ignore
        for future in concurrent.futures.as_completed(tasks):  # type: ignore
            return_code, output = future.result()
            paasta_print('\n'.join(output))
            return_codes.append(return_code)

    return max(return_codes)
