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
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple

import a_sync
import pysensu_yelp
import requests_cache
from marathon.exceptions import MarathonHttpError
from marathon.models.app import MarathonApp
from marathon.models.app import MarathonTask
from mypy_extensions import Arg
from mypy_extensions import DefaultNamedArg
from requests.exceptions import HTTPError
from requests.exceptions import ReadTimeout

from paasta_tools import bounce_lib
from paasta_tools import drain_lib
from paasta_tools import marathon_tools
from paasta_tools import monitoring_tools
from paasta_tools.marathon_tools import get_num_at_risk_tasks
from paasta_tools.marathon_tools import kill_given_tasks
from paasta_tools.marathon_tools import MarathonClient
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


LogDeployError = Callable[[Arg(str, "errormsg"), DefaultNamedArg(str, "level")], None]


LogBounceAction = Callable[[Arg(str, "line"), DefaultNamedArg(str, "level")], None]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Creates marathon jobs.")
    parser.add_argument(
        "service_instance_list",
        nargs="+",
        help="The list of marathon service instances to create or update",
        metavar="SERVICE%sINSTANCE" % SPACER,
    )
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=marathon_tools.DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", dest="verbose", default=False
    )
    args = parser.parse_args()
    return args


def send_event(
    name: str,
    instance: str,
    soa_dir: str,
    status: int,
    output: str,
    system_paasta_config: SystemPaastaConfig,
    marathon_service_config: marathon_tools.MarathonServiceConfig,
) -> None:
    """Send an event to sensu via pysensu_yelp with the given information.

    :param name: The service name the event is about
    :param instance: The instance of the service the event is about
    :param soa_dir: The service directory to read monitoring information from
    :param status: The status to emit for this event
    :param output: The output to emit for this event
    """
    if system_paasta_config is None:
        system_paasta_config = load_system_paasta_config()
    cluster = system_paasta_config.get_cluster()

    if marathon_service_config is None:
        marathon_service_config = marathon_tools.load_marathon_service_config(
            name, instance, cluster, soa_dir=soa_dir, load_deployments=False
        )
    monitoring_overrides = marathon_service_config.get_monitoring()
    # In order to let sensu know how often to expect this check to fire,
    # we need to set the ``check_every`` to the frequency of our cron job, which
    # is 10s.
    monitoring_overrides["check_every"] = "10s"
    # Most setup_marathon_job failures are transient and represent issues
    # that will probably be fixed eventually, so we set an alert_after
    # to suppress extra noise
    monitoring_overrides["alert_after"] = "10m"
    check_name = "setup_marathon_job.%s" % compose_job_id(name, instance)
    monitoring_tools.send_event(
        name,
        check_name,
        monitoring_overrides,
        status,
        output,
        soa_dir,
        system_paasta_config=system_paasta_config,
    )


def drain_tasks_and_find_tasks_to_kill(
    tasks_to_drain: Collection[Tuple[MarathonTask, MarathonClient]],
    already_draining_tasks: Collection[Tuple[MarathonTask, MarathonClient]],
    drain_method: drain_lib.DrainMethod,
    log_bounce_action: LogBounceAction,
    bounce_method: str,
    at_risk_tasks: Collection[Tuple[MarathonTask, MarathonClient]],
) -> Set[Tuple[MarathonTask, MarathonClient]]:
    """Drain the tasks_to_drain, and return the set of tasks that are safe to kill."""
    all_draining_tasks: Set[Tuple[MarathonTask, MarathonClient]] = set(
        already_draining_tasks
    ) | set(at_risk_tasks)
    tasks_to_kill: Set[Tuple[MarathonTask, MarathonClient]] = set()

    if len(tasks_to_drain) > 0:
        tasks_to_drain_by_app_id: Dict[str, Set[MarathonTask]] = defaultdict(set)
        for task, client in tasks_to_drain:
            tasks_to_drain_by_app_id[task.app_id].add(task)
        for app_id, tasks in tasks_to_drain_by_app_id.items():
            log_bounce_action(
                line="%s bounce draining %d old tasks with app_id %s"
                % (bounce_method, len(tasks), app_id)
            )

        async def drain_and_kill_if_draining_fails(
            task: MarathonTask, client: MarathonClient
        ) -> None:
            all_draining_tasks.add((task, client))
            if task.state == "TASK_UNREACHABLE":
                return
            try:
                await drain_method.drain(task)
            except drain_lib.StatusCodeNotAcceptableError as e:
                log_bounce_action(
                    line=f"{bounce_method} bounce killing task {task.id} "
                    f"due to exception when draining: {e}"
                )
                tasks_to_kill.add((task, client))
            # Any other type of exception is really unexpected, so we need to log
            # The whole traceback for later debugging
            except Exception:
                log_bounce_action(
                    line=f"{bounce_method} bounce killing task {task.id} "
                    f"due to exception when draining: {traceback.format_exc()}"
                )
                tasks_to_kill.add((task, client))

        if tasks_to_drain:
            a_sync.block(
                asyncio.wait,
                [
                    asyncio.ensure_future(drain_and_kill_if_draining_fails(t, c))
                    for t, c in tasks_to_drain
                ],
            )

    async def add_to_tasks_to_kill_if_safe_to_kill(
        task: MarathonTask, client: MarathonClient
    ) -> None:
        try:
            if task.state != "TASK_RUNNING" or await drain_method.is_safe_to_kill(task):
                tasks_to_kill.add((task, client))
                log_bounce_action(
                    line="{} bounce killing not_running or drained task {} {}".format(
                        bounce_method, task.id, task.state
                    )
                )
        except Exception:
            tasks_to_kill.add((task, client))
            log_bounce_action(
                line=f"{bounce_method} bounce killing task {task.id} "
                f"due to exception in is_safe_to_kill: {traceback.format_exc()}"
            )

    if all_draining_tasks:
        a_sync.block(
            asyncio.wait,
            [
                asyncio.ensure_future(add_to_tasks_to_kill_if_safe_to_kill(t, c))
                for t, c in all_draining_tasks
            ],
        )
    return tasks_to_kill


def old_app_tasks_to_task_client_pairs(
    old_app_tasks: Mapping[Tuple[str, MarathonClient], Set[MarathonTask]],
) -> Set[Tuple[MarathonTask, MarathonClient]]:
    ret: Set[Tuple[MarathonTask, MarathonClient]] = set()
    for (app, client), tasks in old_app_tasks.items():
        for task in tasks:
            ret.add((task, client))
    return ret


def do_bounce(
    bounce_func: bounce_lib.BounceMethod,
    drain_method: drain_lib.DrainMethod,
    config: marathon_tools.FormattedMarathonAppDict,
    new_app_running: bool,
    happy_new_tasks: Sequence[MarathonTask],
    old_app_live_happy_tasks: Mapping[Tuple[str, MarathonClient], Set[MarathonTask]],
    old_app_live_unhappy_tasks: Mapping[Tuple[str, MarathonClient], Set[MarathonTask]],
    old_app_draining_tasks: Mapping[Tuple[str, MarathonClient], Set[MarathonTask]],
    old_app_at_risk_tasks: Mapping[Tuple[str, MarathonClient], Set[MarathonTask]],
    service: str,
    bounce_method: str,
    serviceinstance: str,
    cluster: str,
    instance: str,
    marathon_jobid: str,
    clients: marathon_tools.MarathonClients,
    soa_dir: str,
    job_config: marathon_tools.MarathonServiceConfig,
    bounce_margin_factor: float = 1.0,
    enable_maintenance_reservation: bool = True,
) -> Optional[float]:
    def log_bounce_action(line: str, level: str = "debug") -> None:
        return _log(
            service=service,
            line=line,
            component="deploy",
            level=level,
            cluster=cluster,
            instance=instance,
        )

    # log if we're not in a steady state.
    if any([(not new_app_running), old_app_live_happy_tasks.keys()]):
        log_bounce_action(
            line=" ".join(
                [
                    f"{bounce_method} bounce in progress on {serviceinstance}.",
                    "New marathon app {} {}.".format(
                        marathon_jobid,
                        ("exists" if new_app_running else "not created yet"),
                    ),
                    "%d new tasks to bring up."
                    % (config["instances"] - len(happy_new_tasks)),
                    "%d old tasks receiving traffic and happy."
                    % len(bounce_lib.flatten_tasks(old_app_live_happy_tasks)),
                    "%d old tasks unhappy."
                    % len(bounce_lib.flatten_tasks(old_app_live_unhappy_tasks)),
                    "%d old tasks draining."
                    % len(bounce_lib.flatten_tasks(old_app_draining_tasks)),
                    "%d old tasks at risk."
                    % len(bounce_lib.flatten_tasks(old_app_at_risk_tasks)),
                    "%d old apps." % len(old_app_live_happy_tasks.keys()),
                ]
            ),
            level="event",
        )
    else:
        log.debug("Nothing to do, bounce is in a steady state")

    new_client = clients.get_current_client_for_service(job_config)

    old_non_draining_tasks = (
        list(old_app_tasks_to_task_client_pairs(old_app_live_happy_tasks))
        + list(old_app_tasks_to_task_client_pairs(old_app_live_unhappy_tasks))
        + list(old_app_tasks_to_task_client_pairs(old_app_at_risk_tasks))
    )

    actions = bounce_func(
        new_config=config,
        new_app_running=new_app_running,
        happy_new_tasks=happy_new_tasks,
        old_non_draining_tasks=old_non_draining_tasks,
        margin_factor=bounce_margin_factor,
    )

    if actions["create_app"] and not new_app_running:
        log_bounce_action(
            line=f"{bounce_method} bounce creating new app with app_id {marathon_jobid}"
        )
        with requests_cache.disabled():
            try:
                bounce_lib.create_marathon_app(
                    app_id=marathon_jobid, config=config, client=new_client
                )
            except MarathonHttpError as e:
                if e.status_code == 409:
                    log.warning(
                        "Failed to create, app %s already exists. This means another bounce beat us to it."
                        " Skipping the rest of the bounce for this run" % marathon_jobid
                    )
                    return 60
                raise

    tasks_to_kill = drain_tasks_and_find_tasks_to_kill(
        tasks_to_drain=actions["tasks_to_drain"],
        already_draining_tasks=old_app_tasks_to_task_client_pairs(
            old_app_draining_tasks
        ),
        drain_method=drain_method,
        log_bounce_action=log_bounce_action,
        bounce_method=bounce_method,
        at_risk_tasks=old_app_tasks_to_task_client_pairs(old_app_at_risk_tasks),
    )

    tasks_to_kill_by_client: Dict[MarathonClient, List[MarathonTask]] = defaultdict(
        list
    )
    for task, client in tasks_to_kill:
        tasks_to_kill_by_client[client].append(task)

    for client, tasks in tasks_to_kill_by_client.items():
        kill_given_tasks(
            client=client, task_ids=[task.id for task in tasks], scale=True
        )

    if enable_maintenance_reservation:
        for task in bounce_lib.flatten_tasks(old_app_at_risk_tasks):
            if task in tasks_to_kill:
                hostname = task.host
                try:
                    reserve_all_resources([hostname])
                except HTTPError:
                    log.warning("Failed to reserve resources on %s" % hostname)

    apps_to_kill: List[Tuple[str, MarathonClient]] = []
    for app, client in old_app_live_happy_tasks.keys():
        if app != "/%s" % marathon_jobid or client != new_client:
            live_happy_tasks = old_app_live_happy_tasks[(app, client)]
            live_unhappy_tasks = old_app_live_unhappy_tasks[(app, client)]
            draining_tasks = old_app_draining_tasks[(app, client)]
            at_risk_tasks = old_app_at_risk_tasks[(app, client)]

            remaining_tasks = (
                live_happy_tasks | live_unhappy_tasks | draining_tasks | at_risk_tasks
            )
            for task, _ in tasks_to_kill:
                remaining_tasks.discard(task)

            if 0 == len(remaining_tasks):
                apps_to_kill.append((app, client))

    if apps_to_kill:
        log_bounce_action(
            line="%s bounce removing old unused apps with app_ids: %s"
            % (bounce_method, ", ".join([app for app, client in apps_to_kill]))
        )
        with requests_cache.disabled():
            for app_id, client in apps_to_kill:
                bounce_lib.kill_old_ids([app_id], client)

    all_old_tasks: Set[MarathonTask] = set()
    all_old_tasks = set.union(all_old_tasks, *old_app_live_happy_tasks.values())
    all_old_tasks = set.union(all_old_tasks, *old_app_live_unhappy_tasks.values())
    all_old_tasks = set.union(all_old_tasks, *old_app_draining_tasks.values())
    all_old_tasks = set.union(all_old_tasks, *old_app_at_risk_tasks.values())

    if all_old_tasks or (not new_app_running):
        # Still have work more work to do, try again in 60 seconds
        return 60
    else:
        # log if we appear to be finished
        if all(
            [
                (apps_to_kill or tasks_to_kill),
                apps_to_kill == list(old_app_live_happy_tasks),
                tasks_to_kill == all_old_tasks,
            ]
        ):
            log_bounce_action(
                line="%s bounce on %s finishing. Now running %s"
                % (bounce_method, serviceinstance, marathon_jobid),
                level="event",
            )

            if yelp_meteorite:
                yelp_meteorite.events.emit_event(
                    "deploy.paasta",
                    dimensions={
                        "paasta_cluster": cluster,
                        "paasta_instance": instance,
                        "paasta_service": service,
                    },
                )
        return None


TasksByStateDict = Dict[str, Set[MarathonTask]]


def get_tasks_by_state_for_app(
    app: MarathonApp,
    drain_method: drain_lib.DrainMethod,
    service: str,
    nerve_ns: str,
    bounce_health_params: Dict[str, Any],
    system_paasta_config: SystemPaastaConfig,
    log_deploy_error: LogDeployError,
    draining_hosts: Collection[str],
) -> TasksByStateDict:
    tasks_by_state: TasksByStateDict = {
        "happy": set(),
        "unhappy": set(),
        "draining": set(),
        "at_risk": set(),
    }

    happy_tasks = bounce_lib.get_happy_tasks(
        app, service, nerve_ns, system_paasta_config, **bounce_health_params
    )

    async def categorize_task(task: MarathonTask) -> None:
        try:
            is_draining = await drain_method.is_draining(task)
        except Exception as e:
            log_deploy_error(
                f"Ignoring {type(e).__name__} exception during is_draining of task "
                f"{task.id} {e.args}. Treating task as 'unhappy'."
            )
            state = "unhappy"
        else:
            if is_draining is True:
                state = "draining"
            elif task in happy_tasks:
                if task.host in draining_hosts:
                    state = "at_risk"
                else:
                    state = "happy"
            else:
                state = "unhappy"
        tasks_by_state[state].add(task)

    if app.tasks:
        a_sync.block(
            asyncio.wait,
            [asyncio.ensure_future(categorize_task(task)) for task in app.tasks],
        )

    return tasks_by_state


def get_tasks_by_state(
    other_apps_with_clients: Collection[Tuple[MarathonApp, MarathonClient]],
    drain_method: drain_lib.DrainMethod,
    service: str,
    nerve_ns: str,
    bounce_health_params: Dict[str, Any],
    system_paasta_config: SystemPaastaConfig,
    log_deploy_error: LogDeployError,
    draining_hosts: Collection[str],
) -> Tuple[
    Dict[Tuple[str, MarathonClient], Set[MarathonTask]],
    Dict[Tuple[str, MarathonClient], Set[MarathonTask]],
    Dict[Tuple[str, MarathonClient], Set[MarathonTask]],
    Dict[Tuple[str, MarathonClient], Set[MarathonTask]],
]:
    """Split tasks from old apps into 4 categories:
      - live (not draining) and happy (according to get_happy_tasks)
      - live (not draining) and unhappy
      - draining
      - at-risk (running on a host marked draining in Mesos in preparation for maintenance)
    """

    old_app_live_happy_tasks = {}
    old_app_live_unhappy_tasks = {}
    old_app_draining_tasks = {}
    old_app_at_risk_tasks = {}

    for app, client in other_apps_with_clients:

        tasks_by_state = get_tasks_by_state_for_app(
            app=app,
            drain_method=drain_method,
            service=service,
            nerve_ns=nerve_ns,
            bounce_health_params=bounce_health_params,
            system_paasta_config=system_paasta_config,
            log_deploy_error=log_deploy_error,
            draining_hosts=draining_hosts,
        )

        old_app_live_happy_tasks[(app.id, client)] = tasks_by_state["happy"]
        old_app_live_unhappy_tasks[(app.id, client)] = tasks_by_state["unhappy"]
        old_app_draining_tasks[(app.id, client)] = tasks_by_state["draining"]
        old_app_at_risk_tasks[(app.id, client)] = tasks_by_state["at_risk"]

    return (
        old_app_live_happy_tasks,
        old_app_live_unhappy_tasks,
        old_app_draining_tasks,
        old_app_at_risk_tasks,
    )


def undrain_tasks(
    to_undrain: Collection[MarathonTask],
    leave_draining: Collection[MarathonTask],
    drain_method: drain_lib.DrainMethod,
    log_deploy_error: LogDeployError,
) -> None:
    # If any tasks on the new app happen to be draining (e.g. someone reverts to an older version with
    # `paasta mark-for-deployment`), then we should undrain them.

    async def undrain_task(task: MarathonTask) -> None:
        if task not in leave_draining:
            if task.state == "TASK_UNREACHABLE":
                return
            try:
                await drain_method.stop_draining(task)
            except Exception:
                log_deploy_error(
                    f"Ignoring exception during stop_draining of task {task.id}: {traceback.format_exc()}"
                )

    if to_undrain:
        a_sync.block(
            asyncio.wait,
            [asyncio.ensure_future(undrain_task(task)) for task in to_undrain],
        )


def deploy_service(
    service: str,
    instance: str,
    marathon_jobid: str,
    config: marathon_tools.FormattedMarathonAppDict,
    clients: marathon_tools.MarathonClients,
    marathon_apps_with_clients: Sequence[Tuple[MarathonApp, MarathonClient]],
    bounce_method: str,
    drain_method_name: str,
    drain_method_params: Dict[str, Any],
    nerve_ns: str,
    registrations: List[str],
    bounce_health_params: Dict[str, Any],
    soa_dir: str,
    job_config: marathon_tools.MarathonServiceConfig,
    system_paasta_config: Optional[SystemPaastaConfig] = None,
    bounce_margin_factor: float = 1.0,
) -> Tuple[int, str, Optional[float]]:
    """Deploy the service to marathon, either directly or via a bounce if needed.
    Called by setup_service when it's time to actually deploy.

    :param service: The name of the service to deploy
    :param instance: The instance of the service to deploy
    :param marathon_jobid: Full id of the marathon job
    :param config: The complete configuration dict to send to marathon
    :param clients: A MarathonClients object
    :param bounce_method: The bounce method to use, if needed
    :param drain_method_name: The name of the traffic draining method to use.
    :param nerve_ns: The nerve namespace to look in.
    :param bounce_health_params: A dictionary of options for bounce_lib.get_happy_tasks.
    :param bounce_margin_factor: the multiplication factor used to calculate the number of instances to be drained
    :returns: A tuple of (status, output, bounce_in_seconds) to be used with send_sensu_event"""

    def log_deploy_error(errormsg: str, level: str = "event") -> None:
        return _log(
            service=service,
            line=errormsg,
            component="deploy",
            level="event",
            cluster=cluster,
            instance=instance,
        )

    if system_paasta_config is None:
        system_paasta_config = load_system_paasta_config()

    cluster = system_paasta_config.get_cluster()
    existing_apps_with_clients = marathon_tools.get_matching_apps_with_clients(
        service=service,
        instance=instance,
        marathon_apps_with_clients=marathon_apps_with_clients,
    )

    new_client = clients.get_current_client_for_service(job_config)

    new_apps_with_clients_list: List[Tuple[MarathonApp, MarathonClient]] = []
    other_apps_with_clients: List[Tuple[MarathonApp, MarathonClient]] = []

    for a, c in existing_apps_with_clients:
        if a.id == "/%s" % config["id"] and c == new_client:
            new_apps_with_clients_list.append((a, c))
        else:
            other_apps_with_clients.append((a, c))

    serviceinstance = f"{service}.{instance}"

    if new_apps_with_clients_list:
        new_app, new_client = new_apps_with_clients_list[0]
        if len(new_apps_with_clients_list) != 1:
            raise ValueError(
                "Only expected one app per ID per shard; found %d"
                % len(new_apps_with_clients_list)
            )
        new_app_running = True
        happy_new_tasks = bounce_lib.get_happy_tasks(
            new_app, service, nerve_ns, system_paasta_config, **bounce_health_params
        )
    else:
        new_app_running = False
        happy_new_tasks = []

    try:
        drain_method = drain_lib.get_drain_method(
            drain_method_name,
            service=service,
            instance=instance,
            registrations=registrations,
            drain_method_params=drain_method_params,
        )
    except KeyError:
        errormsg = "ERROR: drain_method not recognized: {}. Must be one of ({})".format(
            drain_method_name, ", ".join(drain_lib.list_drain_methods())
        )
        log_deploy_error(errormsg)
        return (1, errormsg, None)

    try:
        draining_hosts = get_draining_hosts(system_paasta_config=system_paasta_config)
    except ReadTimeout as e:
        errormsg = (
            "ReadTimeout encountered trying to get draining hosts: %s. Continuing with bounce assuming no tasks at-risk"
            % e
        )
        log_deploy_error(errormsg)
        draining_hosts = []

    (
        old_app_live_happy_tasks,
        old_app_live_unhappy_tasks,
        old_app_draining_tasks,
        old_app_at_risk_tasks,
    ) = get_tasks_by_state(
        other_apps_with_clients=other_apps_with_clients,
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
    for a, c in other_apps_with_clients:
        marathon_tools.take_up_slack(app=a, client=c)

    num_at_risk_tasks = 0
    if new_app_running:
        num_at_risk_tasks = get_num_at_risk_tasks(
            new_app, draining_hosts=draining_hosts
        )
        if new_app.instances < config["instances"] + num_at_risk_tasks:
            log.info(
                "Scaling %s up from %d to %d instances."
                % (
                    new_app.id,
                    new_app.instances,
                    config["instances"] + num_at_risk_tasks,
                )
            )
            new_client.scale_app(
                app_id=new_app.id,
                instances=config["instances"] + num_at_risk_tasks,
                force=True,
            )
        # If we have more than the specified number of instances running, we will want to drain some of them.
        # We will start by draining any tasks running on at-risk hosts.
        elif new_app.instances > config["instances"]:
            num_tasks_to_scale = max(
                min(len(new_app.tasks), new_app.instances) - config["instances"], 0
            )
            task_dict = get_tasks_by_state_for_app(
                app=new_app,
                drain_method=drain_method,
                service=service,
                nerve_ns=nerve_ns,
                bounce_health_params=bounce_health_params,
                system_paasta_config=system_paasta_config,
                log_deploy_error=log_deploy_error,
                draining_hosts=draining_hosts,
            )
            scaling_app_happy_tasks = list(task_dict["happy"])
            scaling_app_unhappy_tasks = list(task_dict["unhappy"])
            scaling_app_draining_tasks = list(task_dict["draining"])
            scaling_app_at_risk_tasks = list(task_dict["at_risk"])

            tasks_to_move_draining = min(
                len(scaling_app_draining_tasks), num_tasks_to_scale
            )
            old_app_draining_tasks[(new_app.id, new_client)] = set(
                scaling_app_draining_tasks[:tasks_to_move_draining]
            )
            num_tasks_to_scale = num_tasks_to_scale - tasks_to_move_draining

            tasks_to_move_unhappy = min(
                len(scaling_app_unhappy_tasks), num_tasks_to_scale
            )
            old_app_live_unhappy_tasks[(new_app.id, new_client)] = set(
                scaling_app_unhappy_tasks[:tasks_to_move_unhappy]
            )
            num_tasks_to_scale = num_tasks_to_scale - tasks_to_move_unhappy

            tasks_to_move_at_risk = min(
                len(scaling_app_at_risk_tasks), num_tasks_to_scale
            )
            old_app_at_risk_tasks[(new_app.id, new_client)] = set(
                scaling_app_at_risk_tasks[:tasks_to_move_at_risk]
            )
            num_tasks_to_scale = num_tasks_to_scale - tasks_to_move_at_risk

            tasks_to_move_happy = min(len(scaling_app_happy_tasks), num_tasks_to_scale)
            old_app_live_happy_tasks[(new_app.id, new_client)] = set(
                scaling_app_happy_tasks[:tasks_to_move_happy]
            )
            happy_new_tasks = scaling_app_happy_tasks[tasks_to_move_happy:]

            # slack represents remaining the extra remaining instances that are configured
            # in marathon that don't have a launched task yet. When scaling down we want to
            # reduce this slack so marathon doesn't get a chance to launch a new task in
            # that space that we will then have to drain and kill again.
            marathon_tools.take_up_slack(client=new_client, app=new_app)

        # TODO: don't take actions in deploy_service.
        undrain_tasks(
            to_undrain=new_app.tasks,
            leave_draining=old_app_draining_tasks.get((new_app.id, new_client), []),
            drain_method=drain_method,
            log_deploy_error=log_deploy_error,
        )

    # log all uncaught exceptions and raise them again
    try:
        try:
            bounce_func = bounce_lib.get_bounce_method_func(bounce_method)
        except KeyError:
            errormsg = (
                "ERROR: bounce_method not recognized: %s. Must be one of (%s)"
                % (bounce_method, ", ".join(bounce_lib.list_bounce_methods()))
            )
            log_deploy_error(errormsg)
            return (1, errormsg, None)

        bounce_again_in_seconds = do_bounce(
            bounce_func=bounce_func,
            drain_method=drain_method,
            config=config,
            new_app_running=new_app_running,
            happy_new_tasks=happy_new_tasks,
            old_app_live_happy_tasks=old_app_live_happy_tasks,
            old_app_live_unhappy_tasks=old_app_live_unhappy_tasks,
            old_app_draining_tasks=old_app_draining_tasks,
            old_app_at_risk_tasks=old_app_at_risk_tasks,
            service=service,
            bounce_method=bounce_method,
            serviceinstance=serviceinstance,
            cluster=cluster,
            instance=instance,
            marathon_jobid=marathon_jobid,
            clients=clients,
            soa_dir=soa_dir,
            job_config=job_config,
            bounce_margin_factor=bounce_margin_factor,
            enable_maintenance_reservation=system_paasta_config.get_maintenance_resource_reservation_enabled(),
        )
    except bounce_lib.LockHeldException:
        logline = f"Failed to get lock to create marathon app for {service}.{instance}"
        log_deploy_error(logline, level="debug")
        return (0, "Couldn't get marathon lock, skipping until next time", None)
    except Exception:
        logline = "Exception raised during deploy of service {}:\n{}".format(
            service, traceback.format_exc()
        )
        log_deploy_error(logline, level="debug")
        raise
    if num_at_risk_tasks:
        bounce_again_in_seconds = 60
    elif new_app_running:
        if new_app.instances > config["instances"]:
            bounce_again_in_seconds = 60
    return (0, "Service deployed.", bounce_again_in_seconds)


def setup_service(
    service: str,
    instance: str,
    clients: marathon_tools.MarathonClients,
    job_config: marathon_tools.MarathonServiceConfig,
    marathon_apps_with_clients: Sequence[Tuple[MarathonApp, MarathonClient]],
    soa_dir: str,
    system_paasta_config: Optional[SystemPaastaConfig] = None,
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
        marathon_app_dict = job_config.format_marathon_app_dict(
            system_paasta_config=system_paasta_config
        )
    except NoDockerImageError:
        error_msg = (
            "Docker image for {0}.{1} not in deployments.json. Exiting. Has Jenkins deployed it?\n"
        ).format(service, instance)
        log.error(error_msg)
        return (1, error_msg, None)

    full_id = marathon_app_dict["id"]
    service_namespace_config = marathon_tools.load_service_namespace_config(
        service=service, namespace=job_config.get_nerve_namespace(), soa_dir=soa_dir
    )

    log.info("Desired Marathon instance id: %s", full_id)
    return deploy_service(
        service=service,
        instance=instance,
        marathon_jobid=full_id,
        config=marathon_app_dict,
        clients=clients,
        marathon_apps_with_clients=marathon_apps_with_clients,
        bounce_method=job_config.get_bounce_method(),
        drain_method_name=job_config.get_drain_method(service_namespace_config),
        drain_method_params=job_config.get_drain_method_params(
            service_namespace_config
        ),
        nerve_ns=job_config.get_nerve_namespace(),
        registrations=job_config.get_registrations(),
        bounce_health_params=job_config.get_bounce_health_params(
            service_namespace_config
        ),
        soa_dir=soa_dir,
        job_config=job_config,
        system_paasta_config=system_paasta_config,
        bounce_margin_factor=job_config.get_bounce_margin_factor(),
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
    clients = marathon_tools.get_marathon_clients(
        marathon_tools.get_marathon_servers(system_paasta_config)
    )
    unique_clients = clients.get_all_clients()
    marathon_apps_with_clients = marathon_tools.get_marathon_apps_with_clients(
        unique_clients, embed_tasks=True
    )

    num_failed_deployments = 0
    for service_instance in args.service_instance_list:
        try:
            service, instance, _, __ = decompose_job_id(service_instance)
        except InvalidJobNameError:
            log.error(
                f"Invalid service instance specified ({service_instance}). Format is service{SPACER}instance."
            )
            num_failed_deployments = num_failed_deployments + 1
        else:
            if deploy_marathon_service(
                service, instance, clients, soa_dir, marathon_apps_with_clients
            )[0]:
                num_failed_deployments = num_failed_deployments + 1

    requests_cache.uninstall_cache()

    log.debug(
        "%d out of %d service.instances failed to deploy."
        % (num_failed_deployments, len(args.service_instance_list))
    )

    sys.exit(1 if num_failed_deployments else 0)


def deploy_marathon_service(
    service: str,
    instance: str,
    clients: marathon_tools.MarathonClients,
    soa_dir: str,
    marathon_apps_with_clients: Optional[Sequence[Tuple[MarathonApp, MarathonClient]]],
    system_paasta_config: Optional[SystemPaastaConfig] = None,
) -> Tuple[int, float]:
    """deploy the service instance given and process return code
    if there was an error we send a sensu alert.

    :param service: The service name to setup
    :param instance: The instance of the service to setup
    :param clients: A MarathonClients object
    :param soa_dir: Path to yelpsoa configs
    :param marathon_apps: A list of all marathon app objects
    :returns: A tuple of (status, bounce_in_seconds) to be used by paasta-deployd
        bounce_in_seconds instructs how long until the deployd should try another bounce
        None means that it is in a steady state and doesn't need to bounce again
    """
    if system_paasta_config is None:
        system_paasta_config = load_system_paasta_config()

    short_id = marathon_tools.format_job_id(service, instance)
    try:
        with bounce_lib.bounce_lock_zookeeper(
            short_id, system_paasta_config=system_paasta_config
        ):
            try:
                service_instance_config = marathon_tools.load_marathon_service_config_no_cache(
                    service,
                    instance,
                    system_paasta_config.get_cluster(),
                    soa_dir=soa_dir,
                )
            except NoDeploymentsAvailable:
                log.debug(
                    "No deployments found for %s.%s in cluster %s. Skipping."
                    % (service, instance, system_paasta_config.get_cluster())
                )
                return 0, None
            except NoConfigurationForServiceError:
                error_msg = (
                    "Could not read marathon configuration file for %s.%s in cluster %s"
                    % (service, instance, system_paasta_config.get_cluster())
                )
                log.error(error_msg)
                return 1, None

            if marathon_apps_with_clients is None:
                marathon_apps_with_clients = marathon_tools.get_marathon_apps_with_clients(
                    clients=clients.get_all_clients_for_service(
                        job_config=service_instance_config
                    ),
                    service_name=service,
                    instance_name=instance,
                    embed_tasks=True,
                )

            try:
                with a_sync.idle_event_loop():
                    status, output, bounce_again_in_seconds = setup_service(
                        service=service,
                        instance=instance,
                        clients=clients,
                        job_config=service_instance_config,
                        marathon_apps_with_clients=marathon_apps_with_clients,
                        soa_dir=soa_dir,
                        system_paasta_config=system_paasta_config,
                    )
                sensu_status = (
                    pysensu_yelp.Status.CRITICAL if status else pysensu_yelp.Status.OK
                )
                send_event(
                    service,
                    instance,
                    soa_dir,
                    sensu_status,
                    output,
                    system_paasta_config,
                    service_instance_config,
                )
                return 0, bounce_again_in_seconds
            except (
                KeyError,
                TypeError,
                AttributeError,
                InvalidInstanceConfig,
                NoSlavesAvailableError,
            ):
                error_str = traceback.format_exc()
                log.error(error_str)
                send_event(
                    service,
                    instance,
                    soa_dir,
                    pysensu_yelp.Status.CRITICAL,
                    error_str,
                    system_paasta_config,
                    service_instance_config,
                )
                return 1, None
    except bounce_lib.LockHeldException:
        log.error("Instance %s already being bounced. Exiting", short_id)
        return 0, None


if __name__ == "__main__":
    main()
