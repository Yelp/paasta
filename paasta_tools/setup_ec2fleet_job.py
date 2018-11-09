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
"""
Usage: ./setup_marathon_job.py <service.instance> [options]

Deploy a service instance to Marathon from a configuration file.
Attempts to load the marathon configuration at
/etc/paasta/marathon.json, and read
from the soa_dir /nail/etc/services by default.

This script will attempt to load a service's configuration
from the soa_dir and generate a marathon job configuration for it,
as well as handle deploying that configuration with a bounce strategy
if there's an old version of the service. To determine whether or not
a deployment is 'old', each marathon job has a complete id of
service.instance.configuration_hash, where configuration_hash
is an MD5 hash of the configuration dict to be sent to marathon (without
the configuration_hash in the id field, of course- we change that after
the hash is calculated).

The script will emit a sensu event based on how the deployment went-
if something went wrong, it'll alert the team responsible for the service
(as defined in that service's monitoring.yaml), and it'll send resolves
when the deployment goes alright.

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -v, --verbose: Verbose output
"""
import argparse
import asyncio
import logging
import sys
import traceback
from collections import defaultdict
from typing import Any
from typing import Callable
from typing import Collection
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

import a_sync
import pysensu_yelp
import requests_cache
from mypy_extensions import Arg
from mypy_extensions import DefaultNamedArg
from requests.exceptions import HTTPError
from requests.exceptions import ReadTimeout

from paasta_tools import bounce_lib
from paasta_tools import drain_lib
from paasta_tools import ec2fleet_tools
from ec2fleet_tools import EC2FleetNode
from paasta_tools import monitoring_tools
from paasta_tools.mesos.exceptions import NoSlavesAvailableError
from paasta_tools.mesos_maintenance import get_draining_hosts
from paasta_tools.mesos_maintenance import reserve_all_resources
from paasta_tools.utils import _log
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import InvalidInstanceConfig
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import SPACER
from paasta_tools.utils import SystemPaastaConfig
try:
    import yelp_meteorite
except ImportError:
    # Sorry to any non-yelpers but you won't
    # get metrics emitted as our metrics lib
    # is currently not open source
    yelp_meteorite = None

# Marathon REST API:
# https://github.com/mesosphere/marathon/blob/master/REST.md#post-v2apps

log = logging.getLogger(__name__)


LogDeployError = Callable[[Arg(str, 'errormsg'), DefaultNamedArg(str, 'level')], None]


LogBounceAction = Callable[[Arg(str, 'line'), DefaultNamedArg(str, 'level')], None]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Creates marathon jobs.')
    parser.add_argument(
        'service_instance_list', nargs='+',
        help="The list of marathon service instances to create or update",
        metavar="SERVICE%sINSTANCE" % SPACER,
    )
    parser.add_argument(
        '-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
        default=ec2fleet_tools.DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        dest="verbose", default=False,
    )
    args = parser.parse_args()
    return args


def send_event(name: str, instance: str, soa_dir: str, status: int, output: str) -> None:
    """Send an event to sensu via pysensu_yelp with the given information.

    :param name: The service name the event is about
    :param instance: The instance of the service the event is about
    :param soa_dir: The service directory to read monitoring information from
    :param status: The status to emit for this event
    :param output: The output to emit for this event
    """
    cluster = load_system_paasta_config().get_cluster()
    monitoring_overrides = ec2fleet_tools.load_ec2fleet_service_config(
        name,
        instance,
        cluster,
        soa_dir=soa_dir,
        load_deployments=False,
    ).get_monitoring()
    # In order to let sensu know how often to expect this check to fire,
    # we need to set the ``check_every`` to the frequency of our cron job, which
    # is 10s.
    monitoring_overrides['check_every'] = '10s'
    # Most setup_marathon_job failures are transient and represent issues
    # that will probably be fixed eventually, so we set an alert_after
    # to suppress extra noise
    monitoring_overrides['alert_after'] = '10m'
    check_name = 'setup_marathon_job.%s' % compose_job_id(name, instance)
    monitoring_tools.send_event(name, check_name, monitoring_overrides, status, output, soa_dir)


def drain_nodes_and_find_nodes_to_kill(
    nodes_to_drain: Collection[EC2FleetNode],
    already_draining_nodes: Collection[EC2FleetNode],
    drain_method: drain_lib.DrainMethod,
    log_bounce_action: LogBounceAction,
    bounce_method: str,
    at_risk_nodes: Collection[EC2FleetNode],
) -> Set[EC2FleetNode]:
    """Drain the nodes_to_drain, and return the set of nodes that are safe to kill."""
    all_draining_nodes: Set[EC2FleetNode] = set(already_draining_nodes) | set(at_risk_nodes)
    nodes_to_kill: Set[EC2FleetNode] = set()

    if len(nodes_to_drain) > 0:
        nodes_to_drain_by_fleet_id: Dict[str, Set[EC2FleetNode]] = defaultdict(set)
        for node in nodes_to_drain:
            nodes_to_drain_by_fleet_id[node.aws_id].add(node)
        for aws_id, nodes in nodes_to_drain_by_fleet_id.items():
            log_bounce_action(
                line='%s bounce draining %d old nodes with aws_id %s' %
                (bounce_method, len(nodes), aws_id),
            )

        async def drain_and_kill_if_draining_fails(node: EC2FleetNode) -> None:
            all_draining_nodes.add(node)
            if node.state == 'TASK_UNREACHABLE':
                return
            try:
                await drain_method.drain(node)
            except Exception:
                log_bounce_action(
                    line=f"{bounce_method} bounce killing node {node.id} "
                         f"due to exception when draining: {traceback.format_exc()}",
                )
                nodes_to_kill.add(node)

        if nodes_to_drain:
            a_sync.block(
                asyncio.wait,
                [asyncio.ensure_future(drain_and_kill_if_draining_fails(node)) for node in nodes_to_drain],
            )

    async def add_to_nodes_to_kill_if_safe_to_kill(node: EC2FleetNode) -> None:
        try:
            if node.state != 'TASK_RUNNING' or await drain_method.is_safe_to_kill(node):
                nodes_to_kill.add(node)
                log_bounce_action(
                    line='{} bounce killing not_running or drained node {} {}'.format(
                        bounce_method, node.id, node.state,
                    ),
                )
        except Exception:
            nodes_to_kill.add(node)
            log_bounce_action(
                line=f'{bounce_method} bounce killing node {node.id} '
                     f'due to exception in is_safe_to_kill: {traceback.format_exc()}',
            )

    if all_draining_nodes:
        a_sync.block(
            asyncio.wait,
            [asyncio.ensure_future(add_to_nodes_to_kill_if_safe_to_kill(node)) for node in all_draining_nodes],
        )
    return nodes_to_kill


def join_old_fleet_nodes(
    old_fleet_nodes: Dict[ec2fleet_tools.EC2Fleet, Set[EC2FleetNode]],
) -> Set[EC2FleetNode]:
    ret: Set[EC2FleetNode] = set()
    for nodes in old_fleet_nodes.values():
        for node in nodes:
            ret.add(node)
    return ret


def sum_weights(
    nodes: Collection[EC2FleetNode],
) -> float:
    return sum([n.weight for n in nodes])


NodesByStateDict = Dict[str, Set[EC2FleetNode]]


def get_nodes_by_state_for_fleet(
    fleet: ec2fleet_tools.EC2Fleet,
    drain_method: drain_lib.DrainMethod,
    service: str,
    nerve_ns: str,
    bounce_health_params: Dict[str, Any],
    system_paasta_config: SystemPaastaConfig,
    log_deploy_error: LogDeployError,
    draining_hosts: Collection[str],
) -> NodesByStateDict:
    nodes_by_state: NodesByStateDict = {
        'happy': set(),
        'unhappy': set(),
        'draining': set(),
        'at_risk': set(),
    }

    # happy_nodes = bounce_lib.get_happy_tasks(fleet, service, nerve_ns, system_paasta_config, **bounce_health_params)
    happy_nodes = get_happy_nodes(fleet, service, nerve_ns, system_paasta_config)

    async def categorize_node(node: EC2FleetNode) -> None:
        try:
            is_draining = await drain_method.is_draining(node)
        except Exception:
            log_deploy_error(
                f"Ignoring exception during is_draining of node {node.id}: "
                f"{traceback.format_exc()}. Treating node as 'unhappy'.",
            )
            state = 'unhappy'
        else:
            if is_draining is True:
                state = 'draining'
            elif node in happy_nodes:
                if node.host in draining_hosts:
                    state = 'at_risk'
                else:
                    state = 'happy'
            else:
                state = 'unhappy'
        nodes_by_state[state].add(node)

    if fleet.nodes:
        a_sync.block(
            asyncio.wait,
            [asyncio.ensure_future(categorize_node(node)) for node in fleet.nodes],
        )

    return nodes_by_state


def get_nodes_by_state(
    other_fleets: Collection[ec2fleet_tools.EC2Fleet],
    drain_method: drain_lib.DrainMethod,
    service: str,
    nerve_ns: str,
    bounce_health_params: Dict[str, Any],
    system_paasta_config: SystemPaastaConfig,
    log_deploy_error: LogDeployError,
    draining_hosts: Collection[str],
) -> Tuple[
    Dict[ec2fleet_tools.EC2Fleet, Set[EC2FleetNode]],
    Dict[ec2fleet_tools.EC2Fleet, Set[EC2FleetNode]],
    Dict[ec2fleet_tools.EC2Fleet, Set[EC2FleetNode]],
    Dict[ec2fleet_tools.EC2Fleet, Set[EC2FleetNode]],
]:
    """Split nodes from old apps into 4 categories:
      - live (not draining) and happy (according to get_happy_nodes)
      - live (not draining) and unhappy
      - draining
      - at-risk (running on a host marked draining in Mesos in preparation for maintenance)
    """

    old_fleet_live_happy_nodes = {}
    old_fleet_live_unhappy_nodes = {}
    old_fleet_draining_nodes = {}
    old_fleet_at_risk_nodes = {}

    for fleet in other_fleets:

        nodes_by_state = get_nodes_by_state_for_fleet(
            fleet=fleet,
            drain_method=drain_method,
            service=service,
            nerve_ns=nerve_ns,
            bounce_health_params=bounce_health_params,
            system_paasta_config=system_paasta_config,
            log_deploy_error=log_deploy_error,
            draining_hosts=draining_hosts,
        )

        old_fleet_live_happy_nodes[fleet] = nodes_by_state['happy']
        old_fleet_live_unhappy_nodes[fleet] = nodes_by_state['unhappy']
        old_fleet_draining_nodes[fleet] = nodes_by_state['draining']
        old_fleet_at_risk_nodes[fleet] = nodes_by_state['at_risk']

    return old_fleet_live_happy_nodes, old_fleet_live_unhappy_nodes, old_fleet_draining_nodes, old_fleet_at_risk_nodes


def undrain_nodes(
    to_undrain: Collection[EC2FleetNode],
    leave_draining: Collection[EC2FleetNode],
    drain_method: drain_lib.DrainMethod,
    log_deploy_error: LogDeployError,
) -> None:
    # If any nodes on the new fleet happen to be draining (e.g. someone reverts to an older version with
    # `paasta mark-for-deployment`), then we should undrain them.

    async def undrain_node(node: EC2FleetNode) -> None:
        if node not in leave_draining:
            if node.state == 'TASK_UNREACHABLE':
                return
            try:
                await drain_method.stop_draining(node)
            except Exception as e:
                log_deploy_error(f"Ignoring exception during stop_draining of node {node.id}: {traceback.format_exc()}")

    if to_undrain:
        a_sync.block(
            asyncio.wait,
            [asyncio.ensure_future(undrain_node(node)) for node in to_undrain],
        )


def get_happy_nodes(
    fleet: ec2fleet_tools.EC2Fleet,
    service: str,
    nerve_ns: str,
    system_paasta_config: SystemPaastaConfig,
    haproxy_min_fraction_up: float = 1.0,
) -> Collection[ec2fleet_tools.EC2FleetNode]:
    return bounce_lib.filter_tasks_in_smartstack(
        fleet.nodes,
        service,
        nerve_ns,
        system_paasta_config,
        haproxy_min_fraction_up=haproxy_min_fraction_up,
    )


def main() -> None:
    """Attempt to set up a list of marathon service instances given.
    Exits 1 if any service.instance deployment failed.
    This is done in the following order:

    - Load the marathon configuration
    - Connect to marathon
    - Do the following for each service.instance:
        - Load the service instance's configuration
        - Create the complete marathon job configuration
        - Deploy/bounce the service
        - Emit an event about the deployment to sensu"""

    args = parse_args()
    soa_dir = args.soa_dir
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    # Setting up transparent cache for http API calls
    requests_cache.install_cache("setup_marathon_jobs", backend="memory")

    system_paasta_config = load_system_paasta_config()
    ec2_client = ec2fleet_tools.get_ec2_client(system_paasta_config)

    num_failed_deployments = 0
    for service_instance in args.service_instance_list:
        try:
            service, instance, _, __ = decompose_job_id(service_instance)
        except InvalidJobNameError:
            log.error("Invalid service instance specified. Format is service%sinstance." % SPACER)
            num_failed_deployments = num_failed_deployments + 1
        else:
            if step_1(service, instance, ec2_client, soa_dir)[0]:
                num_failed_deployments = num_failed_deployments + 1

    requests_cache.uninstall_cache()

    log.debug("%d out of %d service.instances failed to deploy." %
              (num_failed_deployments, len(args.service_instance_list)))

    sys.exit(1 if num_failed_deployments else 0)


def step_1(
    service: str,
    instance: str,
    ec2_client,
    soa_dir: str,
) -> Tuple[int, float]:
    """deploy the service instance given and process return code
    if there was an error we send a sensu alert.

    :param service: The service name to setup
    :param instance: The instance of the service to setup
    :param clients: A MarathonClients object
    :param soa_dir: Path to yelpsoa configs
    :param marathon_apps: A list of all marathon fleet objects
    :returns: A tuple of (status, bounce_in_seconds) to be used by paasta-deployd
        bounce_in_seconds instructs how long until the deployd should try another bounce
        None means that it is in a steady state and doesn't need to bounce again
    """
    short_id = ec2fleet_tools.format_job_id(service, instance)
    try:
        with bounce_lib.bounce_lock_zookeeper(short_id):
            try:
                service_instance_config = ec2fleet_tools.load_ec2fleet_service_config(
                    service,
                    instance,
                    load_system_paasta_config().get_cluster(),
                    soa_dir=soa_dir,
                    load_deployments=True,
                )
            except NoDeploymentsAvailable:
                log.debug("No deployments found for %s.%s in cluster %s. Skipping." %
                          (service, instance, load_system_paasta_config().get_cluster()))
                return 0, None
            except NoConfigurationForServiceError:
                error_msg = "Could not read ec2fleet configuration file for %s.%s in cluster %s" % \
                            (service, instance, load_system_paasta_config().get_cluster())
                log.error(error_msg)
                return 1, None

            try:
                with a_sync.idle_event_loop():
                    status, output, bounce_again_in_seconds = step_2(
                        service=service,
                        instance=instance,
                        ec2_client=ec2_client,
                        job_config=service_instance_config,
                        soa_dir=soa_dir,
                    )
                sensu_status = pysensu_yelp.Status.CRITICAL if status else pysensu_yelp.Status.OK
                send_event(service, instance, soa_dir, sensu_status, output)
                return 0, bounce_again_in_seconds
            except (KeyError, TypeError, AttributeError, InvalidInstanceConfig, NoSlavesAvailableError):
                error_str = traceback.format_exc()
                log.error(error_str)
                send_event(service, instance, soa_dir, pysensu_yelp.Status.CRITICAL, error_str)
                return 1, None
    except bounce_lib.LockHeldException:
        log.error("Instance %s already being bounced. Exiting", short_id)
        return 0, None


def step_2(
    service: str,
    instance: str,
    ec2_client: ec2fleet_tools.Ec2Client,
    job_config: ec2fleet_tools.EC2FleetServiceConfig,
    soa_dir: str,
) -> Tuple[int, str, Optional[float]]:
    """Setup the service instance given and attempt to deploy it, if possible.
    Doesn't do anything if the service is already in Marathon and hasn't changed.
    If it's not, attempt to find old instances of the service and bounce them.

    :param service: The service name to setup
    :param instance: The instance of the service to setup
    :param clients: A MarathonClients object
    :param job_config: The service instance's configuration dict
    :returns: A tuple of (status, output, bounce_in_seconds) to be used with send_sensu_event"""

    log.info("Setting up instance %s for service %s", instance, service)
    try:
        fleet_dict = job_config.format_ec2_fleet_dict()
    except NoDockerImageError:
        error_msg = (
            "Docker image for {0}.{1} not in deployments.json. Exiting. Has Jenkins deployed it?\n"
        ).format(
            service,
            instance,
        )
        log.error(error_msg)
        return (1, error_msg, None)

    paasta_fleet_id = ec2fleet_tools.get_paasta_fleet_id_from_tags(fleet_dict['TagSpecifications'][0]['Tags'])
    service_namespace_config = ec2fleet_tools.load_service_namespace_config(
        service=service, namespace=job_config.get_nerve_namespace(), soa_dir=soa_dir,
    )

    log.info("Desired PaastaFleetId: %s", paasta_fleet_id)
    return step_3(
        service=service,
        instance=instance,
        paasta_fleet_id=paasta_fleet_id,
        config=fleet_dict,
        ec2_client=ec2_client,
        bounce_method=job_config.get_bounce_method(),
        drain_method_name=job_config.get_drain_method(service_namespace_config),
        drain_method_params=job_config.get_drain_method_params(service_namespace_config),
        nerve_ns=job_config.get_nerve_namespace(),
        registrations=job_config.get_registrations(),
        bounce_health_params=job_config.get_bounce_health_params(service_namespace_config),
        soa_dir=soa_dir,
        job_config=job_config,
        bounce_margin_factor=job_config.get_bounce_margin_factor(),
    )


def step_3(
    service: str,
    instance: str,
    paasta_fleet_id: str,
    config: ec2fleet_tools.FormattedEC2FleetDict,
    ec2_client: ec2fleet_tools.Ec2Client,
    bounce_method: str,
    drain_method_name: str,
    drain_method_params: Dict[str, Any],
    nerve_ns: str,
    registrations: List[str],
    bounce_health_params: Dict[str, Any],
    soa_dir: str,
    job_config: ec2fleet_tools.EC2FleetServiceConfig,
    bounce_margin_factor: float=1.0,
) -> Tuple[int, str, Optional[float]]:
    """Deploy the service to marathon, either directly or via a bounce if needed.
    Called by step_2 when it's time to actually deploy.

    :param service: The name of the service to deploy
    :param instance: The instance of the service to deploy
    :param paasta_fleet_id: Full id of the marathon job
    :param config: The complete configuration dict to send to marathon
    :param clients: A MarathonClients object
    :param bounce_method: The bounce method to use, if needed
    :param drain_method_name: The name of the traffic draining method to use.
    :param nerve_ns: The nerve namespace to look in.
    :param bounce_health_params: A dictionary of options for bounce_lib.get_happy_nodes.
    :param bounce_margin_factor: the multiplication factor used to calculate the number of instances to be drained
    :returns: A tuple of (status, output, bounce_in_seconds) to be used with send_sensu_event"""

    def log_deploy_error(errormsg: str, level: str='event') -> None:
        return _log(
            service=service,
            line=errormsg,
            component='deploy',
            level='event',
            cluster=cluster,
            instance=instance,
        )

    system_paasta_config = load_system_paasta_config()
    cluster = system_paasta_config.get_cluster()
    existing_fleets = ec2fleet_tools.fetch_matching_fleets(
        service=service,
        instance=instance,
        client=ec2_client,
    )

    new_fleets_list: List[ec2fleet_tools.EC2Fleet] = []
    other_fleets: List[ec2fleet_tools.EC2Fleet] = []

    for fleet in existing_fleets:
        if fleet.paasta_id == paasta_fleet_id:
            new_fleets_list.append(fleet)
        else:
            other_fleets.append(fleet)

    serviceinstance = f"{service}.{instance}"

    if new_fleets_list:
        new_fleet = new_fleets_list[0]
        if len(new_fleets_list) != 1:
            raise ValueError("Only expected one fleet per ID per shard; found %d" % len(new_fleets_list))
        new_fleet_running = True
        happy_new_nodes = get_happy_nodes(
            new_fleet, service, nerve_ns, system_paasta_config,
        )
    else:
        new_fleet_running = False
        happy_new_nodes = []

    try:
        drain_method = drain_lib.get_drain_method(
            drain_method_name,
            service=service,
            instance=instance,
            registrations=registrations,
            drain_method_params=drain_method_params,
        )
    except KeyError:
        errormsg = 'ERROR: drain_method not recognized: %s. Must be one of (%s)' % \
            (drain_method_name, ', '.join(drain_lib.list_drain_methods()))
        log_deploy_error(errormsg)
        return (1, errormsg, None)

    draining_hosts: Collection[str] = []  # TODO: implement this in some EC2 fleet specific way - perhaps checking hacheck for the 'all' service, or perhaps check for ec2 events,

    (
        old_fleet_live_happy_nodes,
        old_fleet_live_unhappy_nodes,
        old_fleet_draining_nodes,
        old_fleet_at_risk_nodes,
    ) = get_nodes_by_state(
        other_fleets=other_fleets,
        drain_method=drain_method,
        service=service,
        nerve_ns=nerve_ns,
        bounce_health_params=bounce_health_params,
        system_paasta_config=system_paasta_config,
        log_deploy_error=log_deploy_error,
        draining_hosts=draining_hosts,
    )

    # The first thing we need to do is take up the "slack" of old apps, to stop
    # them from launching new things that we are going to have to end up draining
    # and killing anyway.

    # TODO: can we put these into some sort of terminated state, to get this same effect?
    for fleet in other_fleets:
        ec2fleet_tools.take_up_slack(fleet=fleet, ec2_client=ec2_client)

    at_risk_capacity = 0
    if new_fleet_running:
        at_risk_capacity = ec2fleet_tools.get_amount_of_at_risk_weight(new_fleet, draining_hosts=draining_hosts)
        if new_fleet.target_capacity < config['TargetCapacitySpecification']['TotalTargetCapacity'] + at_risk_capacity:
            log.info("Scaling %s up from %d to %d capacity." %
                     (new_fleet.paasta_id, new_fleet.target_capacity, config['TargetCapacitySpecification']['TotalTargetCapacity'] + at_risk_capacity))
            ec2fleet_tools.scale_fleet(
                aws_id=new_fleet.aws_id,
                new_target_capacity=config['TargetCapacitySpecification']['TotalTargetCapacity'] + at_risk_capacity,
            )
        # If we have more than the specified number of instances running, we will want to drain some of them.
        # We will start by draining any nodes running on at-risk hosts.
        elif new_fleet.target_capacity > config['TargetCapacitySpecification']['TotalTargetCapacity']:
            num_nodes_to_scale = max(min(len(new_fleet.nodes), new_fleet.target_capacity) - config['TargetCapacitySpecification']['TotalTargetCapacity'], 0)
            node_dict = get_nodes_by_state_for_fleet(
                fleet=new_fleet,
                drain_method=drain_method,
                service=service,
                nerve_ns=nerve_ns,
                bounce_health_params=bounce_health_params,
                system_paasta_config=system_paasta_config,
                log_deploy_error=log_deploy_error,
                draining_hosts=draining_hosts,
            )
            scaling_fleet_happy_nodes = list(node_dict['happy'])
            scaling_fleet_unhappy_nodes = list(node_dict['unhappy'])
            scaling_fleet_draining_nodes = list(node_dict['draining'])
            scaling_fleet_at_risk_nodes = list(node_dict['at_risk'])

            nodes_to_move_draining = min(len(scaling_fleet_draining_nodes), num_nodes_to_scale)
            old_fleet_draining_nodes[new_fleet] = set(scaling_fleet_draining_nodes[:nodes_to_move_draining])
            num_nodes_to_scale = num_nodes_to_scale - nodes_to_move_draining

            nodes_to_move_unhappy = min(len(scaling_fleet_unhappy_nodes), num_nodes_to_scale)
            old_fleet_live_unhappy_nodes[new_fleet] = set(
                scaling_fleet_unhappy_nodes[:nodes_to_move_unhappy],
            )
            num_nodes_to_scale = num_nodes_to_scale - nodes_to_move_unhappy

            nodes_to_move_at_risk = min(len(scaling_fleet_at_risk_nodes), num_nodes_to_scale)
            old_fleet_at_risk_nodes[new_fleet] = set(scaling_fleet_at_risk_nodes[:nodes_to_move_at_risk])
            num_nodes_to_scale = num_nodes_to_scale - nodes_to_move_at_risk

            nodes_to_move_happy = min(len(scaling_fleet_happy_nodes), num_nodes_to_scale)
            old_fleet_live_happy_nodes[new_fleet] = set(scaling_fleet_happy_nodes[:nodes_to_move_happy])
            happy_new_nodes = scaling_fleet_happy_nodes[nodes_to_move_happy:]

            # slack represents remaining the extra remaining instances that are configured
            # in marathon that don't have a launched node yet. When scaling down we want to
            # reduce this slack so marathon doesn't get a chance to launch a new node in
            # that space that we will then have to drain and kill again.
            ec2fleet_tools.take_up_slack(fleet=new_fleet, ec2_client=ec2_client)

        # TODO: don't take actions in step_3.
        undrain_nodes(
            to_undrain=new_fleet.nodes,
            leave_draining=old_fleet_draining_nodes.get(new_fleet, []),
            drain_method=drain_method,
            log_deploy_error=log_deploy_error,
        )

    # log all uncaught exceptions and raise them again
    try:
        try:
            bounce_func = bounce_lib.get_bounce_method_func(bounce_method)
        except KeyError:
            errormsg = 'ERROR: bounce_method not recognized: %s. Must be one of (%s)' % \
                (bounce_method, ', '.join(bounce_lib.list_bounce_methods()))
            log_deploy_error(errormsg)
            return (1, errormsg, None)

        bounce_again_in_seconds = step_4(
            bounce_func=bounce_func,
            drain_method=drain_method,
            config=config,
            new_fleet_running=new_fleet_running,
            happy_new_nodes=happy_new_nodes,
            old_fleet_live_happy_nodes=old_fleet_live_happy_nodes,
            old_fleet_live_unhappy_nodes=old_fleet_live_unhappy_nodes,
            old_fleet_draining_nodes=old_fleet_draining_nodes,
            old_fleet_at_risk_nodes=old_fleet_at_risk_nodes,
            service=service,
            bounce_method=bounce_method,
            serviceinstance=serviceinstance,
            cluster=cluster,
            instance=instance,
            paasta_fleet_id=paasta_fleet_id,
            ec2_client=ec2_client,
            soa_dir=soa_dir,
            job_config=job_config,
            bounce_margin_factor=bounce_margin_factor,
            enable_maintenance_reservation=system_paasta_config.get_maintenance_resource_reservation_enabled(),
        )
    except bounce_lib.LockHeldException:
        logline = f'Failed to get lock to create marathon fleet for {service}.{instance}'
        log_deploy_error(logline, level='debug')
        return (0, "Couldn't get marathon lock, skipping until next time", None)
    except Exception:
        logline = 'Exception raised during deploy of service {}:\n{}'.format(service, traceback.format_exc())
        log_deploy_error(logline, level='debug')
        raise
    if at_risk_capacity:
        bounce_again_in_seconds = 60
    elif new_fleet_running:
        if new_fleet.target_capacity > config['TargetCapacitySpecification']['TotalTargetCapacity']:
            bounce_again_in_seconds = 60
    return (0, 'Service deployed.', bounce_again_in_seconds)


def step_4(
    bounce_func: bounce_lib.BounceMethod,
    drain_method: drain_lib.DrainMethod,
    config: ec2fleet_tools.FormattedEC2FleetDict,
    new_fleet_running: bool,
    happy_new_nodes: Collection[EC2FleetNode],
    old_fleet_live_happy_nodes: Dict[ec2fleet_tools.EC2Fleet, Set[EC2FleetNode]],
    old_fleet_live_unhappy_nodes: Dict[ec2fleet_tools.EC2Fleet, Set[EC2FleetNode]],
    old_fleet_draining_nodes: Dict[ec2fleet_tools.EC2Fleet, Set[EC2FleetNode]],
    old_fleet_at_risk_nodes: Dict[ec2fleet_tools.EC2Fleet, Set[EC2FleetNode]],
    service: str,
    bounce_method: str,
    serviceinstance: str,
    cluster: str,
    instance: str,
    paasta_fleet_id: str,
    ec2_client: ec2fleet_tools.Ec2Client,
    soa_dir: str,
    job_config: ec2fleet_tools.EC2FleetServiceConfig,
    bounce_margin_factor: float=1.0,
    enable_maintenance_reservation: bool=True,
) -> Optional[float]:
    def log_bounce_action(line: str, level: str='debug') -> None:
        return _log(
            service=service,
            line=line,
            component='deploy',
            level=level,
            cluster=cluster,
            instance=instance,
        )

    # log if we're not in a steady state.
    if any([
        (not new_fleet_running),
        old_fleet_live_happy_nodes.keys(),
    ]):
        log_bounce_action(
            line=' '.join([
                f'{bounce_method} bounce in progress on {serviceinstance}.',
                'New EC2 fleet {} {}.'.format(paasta_fleet_id, ('exists' if new_fleet_running else 'not created yet')),
                '%f new capacity to bring up.' % (config['TargetCapacitySpecification']['TotalTargetCapacity'] - sum_weights(happy_new_nodes)),
                '%d old nodes (%f weight) receiving traffic and happy.' % (
                    len(bounce_lib.flatten_tasks(old_fleet_live_happy_nodes)),
                    sum_weights(bounce_lib.flatten_tasks(old_fleet_live_happy_nodes)),
                ),
                '%d old nodes (%f weight) unhappy.' % (
                    len(bounce_lib.flatten_tasks(old_fleet_live_unhappy_nodes)),
                    sum_weights(bounce_lib.flatten_tasks(old_fleet_live_unhappy_nodes)),
                ),
                '%d old nodes (%f weight) draining.' % (
                    len(bounce_lib.flatten_tasks(old_fleet_draining_nodes)),
                    sum_weights(bounce_lib.flatten_tasks(old_fleet_draining_nodes)),
                ),
                '%d old nodes (%f weight) at risk.' % (
                    len(bounce_lib.flatten_tasks(old_fleet_at_risk_nodes)),
                    sum_weights(bounce_lib.flatten_tasks(old_fleet_at_risk_nodes)),
                ),
                '%d old fleets.' % len(old_fleet_live_happy_nodes.keys()),
            ]),
            level='event',
        )
    else:
        log.debug("Nothing to do, bounce is in a steady state")

    old_non_draining_nodes = list(
        join_old_fleet_nodes(old_fleet_live_happy_nodes),
    ) + list(
        join_old_fleet_nodes(old_fleet_live_unhappy_nodes),
    ) + list(
        join_old_fleet_nodes(old_fleet_at_risk_nodes),
    )

    actions = bounce_func(
        required_capacity=config['TargetCapacitySpecification']['TotalTargetCapacity'],
        new_app_running=new_fleet_running,
        happy_new_tasks=happy_new_nodes,
        old_non_draining_tasks=old_non_draining_nodes,
        margin_factor=bounce_margin_factor,
    )

    if actions['create_app'] and not new_fleet_running:
        log_bounce_action(
            line=f'{bounce_method} bounce creating new fleet with app_id {paasta_fleet_id}',
        )

        try:
            job_config.create(ec2_client)
        except ec2fleet_tools.EC2FleetAlreadyExistsError:
            log.warning(
                "Failed to create, fleet %s already exists. This means another bounce beat us to it."
                " Skipping the rest of the bounce for this run" % paasta_fleet_id,
            )
            return 60

    nodes_to_kill = drain_nodes_and_find_nodes_to_kill(
        nodes_to_drain=actions['tasks_to_drain'],
        already_draining_nodes=join_old_fleet_nodes(old_fleet_draining_nodes),
        drain_method=drain_method,
        log_bounce_action=log_bounce_action,
        bounce_method=bounce_method,
        at_risk_nodes=join_old_fleet_nodes(old_fleet_at_risk_nodes),
    )

    # TODO(krall): do I need to adjust the capacity down?
    ec2fleet_tools.kill_given_nodes(client=ec2_client, node_ids=[node.id for node in nodes_to_kill])

    if enable_maintenance_reservation:
        for node in bounce_lib.flatten_tasks(old_fleet_at_risk_nodes):
            if node in nodes_to_kill:
                hostname = node.host
                try:
                    reserve_all_resources([hostname])
                except HTTPError:
                    log.warning("Failed to reserve resources on %s" % hostname)

    fleets_to_kill: List[ec2fleet_tools.EC2Fleet] = []

    for fleet in old_fleet_live_happy_nodes.keys():
        if fleet.paasta_id != paasta_fleet_id:
            live_happy_nodes = old_fleet_live_happy_nodes[fleet]
            live_unhappy_nodes = old_fleet_live_unhappy_nodes[fleet]
            draining_nodes = old_fleet_draining_nodes[fleet]
            at_risk_nodes = old_fleet_at_risk_nodes[fleet]

            remaining_nodes = (live_happy_nodes | live_unhappy_nodes | draining_nodes | at_risk_nodes)
            for node, _ in nodes_to_kill:
                remaining_nodes.discard(node)

            if 0 == len(remaining_nodes):
                fleets_to_kill.append(fleet)

    if fleets_to_kill:
        log_bounce_action(
            line='%s bounce removing old unused apps with app_ids: %s' %
            (
                bounce_method,
                ', '.join([f"{fleet.paasta_id}/{fleet.aws_id}" for fleet in fleets_to_kill]),
            ),
        )
        with requests_cache.disabled():
            for fleet in fleets_to_kill:
                ec2fleet_tools.cleanup_fleet(ec2_client, fleet.aws_id)

    all_old_nodes: Set[EC2FleetNode] = set()
    all_old_nodes = set.union(all_old_nodes, *old_fleet_live_happy_nodes.values())
    all_old_nodes = set.union(all_old_nodes, *old_fleet_live_unhappy_nodes.values())
    all_old_nodes = set.union(all_old_nodes, *old_fleet_draining_nodes.values())
    all_old_nodes = set.union(all_old_nodes, *old_fleet_at_risk_nodes.values())

    if all_old_nodes or (not new_fleet_running):
        # Still have work more work to do, try again in 60 seconds
        return 60
    else:
        # log if we appear to be finished
        if all([
            (fleets_to_kill or nodes_to_kill),
            fleets_to_kill == list(old_fleet_live_happy_nodes),
            nodes_to_kill == all_old_nodes,
        ]):
            log_bounce_action(
                line='%s bounce on %s finishing. Now running %s' %
                (
                    bounce_method,
                    serviceinstance,
                    paasta_fleet_id,
                ),
                level='event',
            )

            if yelp_meteorite:
                yelp_meteorite.events.emit_event(
                    'deploy.paasta',
                    dimensions={
                        'paasta_cluster': cluster,
                        'paasta_instance': instance,
                        'paasta_service': service,
                    },
                )
        return None


if __name__ == "__main__":
    main()
