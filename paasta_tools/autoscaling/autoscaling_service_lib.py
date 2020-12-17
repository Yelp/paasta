#!/usr/bin/env python
# Copyright 2015-2019 Yelp Inc.
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
import asyncio
import json
import logging
import struct
import time
from collections import namedtuple
from contextlib import contextmanager
from datetime import datetime
from math import ceil
from math import floor
from typing import Dict
from typing import Iterator
from typing import List
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Tuple

import a_sync
import aiohttp
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from marathon.models.app import MarathonApp
from marathon.models.app import MarathonTask

from paasta_tools.autoscaling.forecasting import get_forecast_policy
from paasta_tools.autoscaling.utils import get_autoscaling_component
from paasta_tools.autoscaling.utils import register_autoscaling_component
from paasta_tools.bounce_lib import filter_tasks_in_smartstack
from paasta_tools.bounce_lib import LockHeldException
from paasta_tools.bounce_lib import LockTimeout
from paasta_tools.bounce_lib import ZK_LOCK_CONNECT_TIMEOUT_S
from paasta_tools.long_running_service_tools import AutoscalingParamsDict
from paasta_tools.long_running_service_tools import load_service_namespace_config
from paasta_tools.long_running_service_tools import ZK_PAUSE_AUTOSCALE_PATH
from paasta_tools.marathon_tools import compose_autoscaling_zookeeper_root
from paasta_tools.marathon_tools import format_job_id
from paasta_tools.marathon_tools import get_marathon_apps_with_clients
from paasta_tools.marathon_tools import get_marathon_clients
from paasta_tools.marathon_tools import get_marathon_servers
from paasta_tools.marathon_tools import is_old_task_missing_healthchecks
from paasta_tools.marathon_tools import is_task_healthy
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.marathon_tools import MESOS_TASK_SPACER
from paasta_tools.marathon_tools import set_instances_for_marathon_service
from paasta_tools.mesos.task import Task
from paasta_tools.mesos_tools import get_all_running_tasks
from paasta_tools.mesos_tools import get_cached_list_of_running_tasks_from_frameworks
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_user_agent
from paasta_tools.utils import list_services
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import mean
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import use_requests_cache
from paasta_tools.utils import ZookeeperPool

try:
    import yelp_meteorite
except ImportError:
    # Sorry to any non-yelpers but you won't
    # get metrics emitted as our metrics library
    # is currently not open source
    yelp_meteorite = None

ServiceAutoscalingInfo = namedtuple(
    "ServiceAutoscalingInfo",
    [
        "current_instances",
        "max_instances",
        "min_instances",
        "current_utilization",
        "target_instances",
    ],
)


SERVICE_METRICS_PROVIDER_KEY = "metrics_provider"
DECISION_POLICY_KEY = "decision_policy"

AUTOSCALING_DELAY = 300
MAX_TASK_DELTA = 0.3

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


def get_service_metrics_provider(name):
    """
    Returns a service metrics provider matching the given name.
    """
    return get_autoscaling_component(name, SERVICE_METRICS_PROVIDER_KEY)


def get_decision_policy(name):
    """
    Returns a decision policy matching the given name.
    Decision policies determine the direction a service needs to be scaled in.
    Each decision policy returns one of the following values:
    -1: autoscale down
    0:  don't autoscale
    1:  autoscale up
    """
    return get_autoscaling_component(name, DECISION_POLICY_KEY)


class MetricsProviderNoDataError(ValueError):
    pass


@register_autoscaling_component("threshold", DECISION_POLICY_KEY)
def threshold_decision_policy(current_instances, error, **kwargs):
    """
    Decides to autoscale up or down by 10% if the service exceeds the upper or lower thresholds
    (see get_error_from_value() for how the thresholds are created)
    """
    autoscaling_amount = max(1, int(current_instances * 0.1))
    if error > 0:
        return autoscaling_amount
    elif abs(error) > 0:
        return -autoscaling_amount
    else:
        return 0


@register_autoscaling_component("proportional", DECISION_POLICY_KEY)
def proportional_decision_policy(
    zookeeper_path,
    current_instances,
    setpoint,
    utilization,
    num_healthy_instances,
    persist_data: bool,
    noop=False,
    offset=0.0,
    forecast_policy="current",
    good_enough_window=None,
    **kwargs,
):
    """Uses a simple proportional model to decide the correct number of instances to scale to, i.e. if load is 110% of
    the setpoint, scales up by 10%. Includes correction for an offset, if your containers have a baseline utilization
    independent of the number of containers.

    The model is: utilization per container = (total load)/(number of containers) + offset.

    total load and offset are measured in the same unit as your metric provider. If you're measuring CPU per container,
    offset is the baseline CPU of an idle container, and total load is the total CPU required across all containers,
    subtracting the offset for each container.

    :param offset: A float (should be between 0.0 and 1.0) representing the expected baseline load for each container.
                   e.g. if the metric you're using is CPU, then how much CPU an idle container would use.
                   This should never be more than your setpoint. (If it takes 50% cpu to run an idle container, we can't
                   get your utilization below 50% no matter how many containers we run.)
    :param forecast_policy: The method for forecasting future load values. Currently, only two forecasters exist:
                            - "current", which assumes that the load will remain the same as the current value for the
                            near future.
                            - "moving_average", which assumes that total load will remain near the average of data
                            points within a window.
    :param good_enough_window: A tuple/array of two utilization values, (low, high). If the utilization per container at
                               the forecasted total load is within this window with the current number of instances,
                               leave the number of instances alone. This can reduce churn. Setpoint should lie within
                               this window.
    """

    forecast_policy_func = get_forecast_policy(forecast_policy)

    current_load = (utilization - offset) * num_healthy_instances

    historical_load = fetch_historical_load(zk_path_prefix=zookeeper_path)
    historical_load.append((time.time(), current_load))
    if persist_data:
        save_historical_load(historical_load, zk_path_prefix=zookeeper_path)

    predicted_load = forecast_policy_func(historical_load, **kwargs)

    desired_number_instances = int(round(predicted_load / (setpoint - offset)))

    # Don't scale down if the current utilization >= the setpoint (or the high point of the good enough window)
    # This prevents the case where the moving_average forcast_policy thinks the service needs to scale
    #  down several times in a row due to under-utilization in the near past
    if desired_number_instances < current_instances:
        if good_enough_window:
            _, high = good_enough_window
            if utilization >= high:
                desired_number_instances = current_instances
        elif utilization >= setpoint:
            desired_number_instances = current_instances

    if good_enough_window:
        low, high = good_enough_window
        predicted_load_per_instance_with_current_instances = (
            predicted_load / current_instances + offset
        )
        if low <= predicted_load_per_instance_with_current_instances <= high:
            desired_number_instances = current_instances

    return (
        desired_number_instances - current_instances
    )  # The calling function wants a delta, not an absolute value.


HISTORICAL_LOAD_SERIALIZATION_FORMAT = "dd"
SIZE_PER_HISTORICAL_LOAD_RECORD = struct.calcsize(HISTORICAL_LOAD_SERIALIZATION_FORMAT)


def zk_historical_load_path(zk_path_prefix):
    return "%s/historical_load" % zk_path_prefix


def save_historical_load(historical_load, zk_path_prefix):
    with ZookeeperPool() as zk:
        historical_load_bytes = serialize_historical_load(historical_load)
        zk.ensure_path(zk_historical_load_path(zk_path_prefix))
        zk.set(zk_historical_load_path(zk_path_prefix), historical_load_bytes)


def serialize_historical_load(historical_load):
    max_records = 1000000 // SIZE_PER_HISTORICAL_LOAD_RECORD
    historical_load = historical_load[-max_records:]
    return b"".join(
        [struct.pack(HISTORICAL_LOAD_SERIALIZATION_FORMAT, *x) for x in historical_load]
    )


def fetch_historical_load(zk_path_prefix):
    with ZookeeperPool() as zk:
        try:
            historical_load_bytes, _ = zk.get(zk_historical_load_path(zk_path_prefix))
            return deserialize_historical_load(historical_load_bytes)
        except NoNodeError:
            return []


def deserialize_historical_load(historical_load_bytes):
    historical_load = []

    for pos in range(0, len(historical_load_bytes), SIZE_PER_HISTORICAL_LOAD_RECORD):
        historical_load.append(
            struct.unpack(
                # unfortunately struct.unpack doesn't like kwargs.
                HISTORICAL_LOAD_SERIALIZATION_FORMAT,
                historical_load_bytes[pos : pos + SIZE_PER_HISTORICAL_LOAD_RECORD],
            )
        )

    return historical_load


async def get_json_body_from_service(host, port, endpoint, session):
    async with session.get(
        f"http://{host}:{port}/{endpoint}", headers={"User-Agent": get_user_agent()}
    ) as response:
        return await response.json()


async def get_http_utilization_for_a_task(
    task, service, instance, endpoint, json_mapper, session
):
    """
    Gets the task utilization by fetching json from an http endpoint
    and applying a function that maps it to a utilization.

    :param task: the Marathon task to get data from
    :param service: service name
    :param endpoint: the http endpoint to get the task stats from
    :param json_mapper: a function that takes a dictionary for a task and returns that task's utilization

    :returns: the service's utilization, from 0 to 1, or None
    """
    try:
        return json_mapper(
            await get_json_body_from_service(
                host=task.host, port=task.ports[0], endpoint=endpoint, session=session
            )
        )
    except aiohttp.ServerTimeoutError:
        # If we time out querying an endpoint, assume the task is fully loaded
        # This won't trigger in the event of DNS error or when a request is refused
        log.error(
            f"Received a timeout when querying {service}.{instance} on {task.host}:{task.ports[0]}. Assuming the service is at full utilization."
        )
        return 1.0
    except Exception as e:
        log.error(
            f"Caught exception when querying {service}.{instance} on {task.host}:{task.ports[0]} : {str(e)}"
        )


@a_sync.to_blocking
async def get_http_utilization_for_all_tasks(
    marathon_service_config, marathon_tasks, endpoint, json_mapper
):
    """
    Gets the mean utilization of a service across all of its tasks by fetching
    json from an http endpoint and applying a function that maps it to a
    utilization

    :param marathon_service_config: the MarathonServiceConfig to get data from
    :param marathon_tasks: Marathon tasks to get data from
    :param endpoint: The http endpoint to get the stats from
    :param json_mapper: A function that takes a dictionary for a task and returns that task's utilization

    :returns: the service's mean utilization, from 0 to 1
    """

    endpoint = endpoint.lstrip("/")
    utilization = []
    service = marathon_service_config.get_service()
    instance = marathon_service_config.get_instance()

    # Using a single aiohttp session reduces the number of errors seen. Launching
    # hundreds of unique sessions seems to increase (timeout) errors.
    # However, using 1 session is slower because the default number of connections
    # is 100, but still seems to be a sane amount.
    async with aiohttp.ClientSession(conn_timeout=10, read_timeout=10) as session:
        futures = [
            asyncio.ensure_future(
                get_http_utilization_for_a_task(
                    task=task,
                    service=service,
                    instance=instance,
                    endpoint=endpoint,
                    json_mapper=json_mapper,
                    session=session,
                )
            )
            for task in marathon_tasks
        ]
        await asyncio.wait(futures)

    for future in futures:
        result = future.result()
        if result is not None:
            utilization.append(result)

    if not utilization:
        raise MetricsProviderNoDataError(
            "Couldn't get any data from http endpoint {} for {}.{}".format(
                endpoint,
                marathon_service_config.service,
                marathon_service_config.instance,
            )
        )
    return mean(utilization)


@register_autoscaling_component("uwsgi", SERVICE_METRICS_PROVIDER_KEY)
def uwsgi_metrics_provider(
    marathon_service_config, marathon_tasks, endpoint="status/uwsgi", **kwargs
):
    """
    Gets the mean utilization of a service across all of its tasks, where
    the utilization of a task is the percentage of non-idle workers as read
    from the UWSGI stats endpoint

    :param marathon_service_config: the MarathonServiceConfig to get data from
    :param marathon_tasks: Marathon tasks to get data from
    :param endpoint: The http endpoint to get the uwsgi stats from

    :returns: the service's mean utilization, from 0 to 1
    """

    def uwsgi_mapper(json):
        workers = json["workers"]
        utilization = [1.0 if worker["status"] != "idle" else 0.0 for worker in workers]
        return mean(utilization)

    return get_http_utilization_for_all_tasks(
        marathon_service_config, marathon_tasks, endpoint, uwsgi_mapper
    )


@register_autoscaling_component("http", SERVICE_METRICS_PROVIDER_KEY)
def http_metrics_provider(
    marathon_service_config, marathon_tasks, endpoint="status", **kwargs
):
    """
    Gets the mean utilization of a service across all of its tasks, where
    the utilization of a task is read from a HTTP endpoint on the host. The
    HTTP endpoint must return JSON with a 'utilization' key with a value from 0
    to 1.

    :param marathon_service_config: the MarathonServiceConfig to get data from
    :param marathon_tasks: Marathon tasks to get data from
    :param endpoint: The http endpoint to get the task utilization from

    :returns: the service's mean utilization, from 0 to 1
    """

    def utilization_mapper(json):
        return float(json["utilization"])

    return get_http_utilization_for_all_tasks(
        marathon_service_config, marathon_tasks, endpoint, utilization_mapper
    )


@register_autoscaling_component("mesos_cpu", SERVICE_METRICS_PROVIDER_KEY)
def mesos_cpu_metrics_provider(
    marathon_service_config,
    system_paasta_config,
    marathon_tasks,
    mesos_tasks,
    log_utilization_data={},
    noop=False,
    **kwargs,
):
    """
    Gets the mean cpu utilization of a service across all of its tasks.

    :param marathon_service_config: the MarathonServiceConfig to get data from
    :param marathon_tasks: Marathon tasks to get data from
    :param mesos_tasks: Mesos tasks to get data from
    :param log_utilization_data: A dict used to transfer utilization data to autoscale_marathon_instance()

    :returns: the service's mean utilization, from 0 to 1
    """

    autoscaling_root = compose_autoscaling_zookeeper_root(
        service=marathon_service_config.service,
        instance=marathon_service_config.instance,
    )
    zk_last_time_path = "%s/cpu_last_time" % autoscaling_root
    zk_last_cpu_data = "%s/cpu_data" % autoscaling_root

    with ZookeeperPool() as zk:
        try:
            last_time = zk.get(zk_last_time_path)[0].decode("utf8")
            last_cpu_data = zk.get(zk_last_cpu_data)[0].decode("utf8")
            log_utilization_data[last_time] = last_cpu_data
            last_time = float(last_time)
            last_cpu_data = (datum for datum in last_cpu_data.split(",") if datum)
        except NoNodeError:
            last_time = 0.0
            last_cpu_data = []

    futures = [asyncio.ensure_future(task.stats()) for task in mesos_tasks]
    if futures:
        a_sync.block(asyncio.wait, futures, timeout=60)

    def results_or_None(fut):
        if fut.exception():
            return None
        else:
            return fut.result()

    mesos_tasks_stats = dict(
        zip(
            [task["id"] for task in mesos_tasks],
            [results_or_None(fut) for fut in futures],
        )
    )

    current_time = int(datetime.now().strftime("%s"))
    time_delta = current_time - last_time

    mesos_cpu_data = {}
    for task_id, stats in mesos_tasks_stats.items():
        if stats is not None:
            try:
                utime = float(stats["cpus_user_time_secs"])
                stime = float(stats["cpus_system_time_secs"])
                limit = float(stats["cpus_limit"]) - 0.1
                mesos_cpu_data[task_id] = (stime + utime) / limit
            except KeyError:
                pass

    if not mesos_cpu_data:
        raise MetricsProviderNoDataError("Couldn't get any cpu data from Mesos")

    cpu_data_csv = ",".join(
        f"{cpu_seconds}:{task_id}" for task_id, cpu_seconds in mesos_cpu_data.items()
    )
    log_utilization_data[str(current_time)] = cpu_data_csv

    if not noop:
        with ZookeeperPool() as zk:
            zk.ensure_path(zk_last_cpu_data)
            zk.ensure_path(zk_last_time_path)
            zk.set(zk_last_cpu_data, str(cpu_data_csv).encode("utf8"))
            zk.set(zk_last_time_path, str(current_time).encode("utf8"))

    utilization = {}
    for datum in last_cpu_data:
        last_cpu_seconds, task_id = datum.split(":")
        if task_id in mesos_cpu_data:
            cputime_delta = mesos_cpu_data[task_id] - float(last_cpu_seconds)
            utilization[task_id] = cputime_delta / time_delta

    if not utilization:
        raise MetricsProviderNoDataError(
            """The mesos_cpu metrics provider doesn't have Zookeeper data for this service.
                                         This is expected for its first run."""
        )

    task_utilization = utilization.values()
    mean_utilization = mean(task_utilization)
    return mean_utilization


def get_error_from_utilization(utilization, setpoint, current_instances):
    """
    Consider scaling up if utilization is above the setpoint
    Consider scaling down if the utilization is below the setpoint AND scaling down wouldn't bring it above the setpoint
    Otherwise don't scale
    """
    max_threshold = setpoint
    min_threshold = max_threshold * (current_instances - 1) / current_instances
    if utilization < min_threshold:
        return utilization - min_threshold
    elif utilization > max_threshold:
        return utilization - max_threshold
    else:
        return 0.0


def get_autoscaling_info(apps_with_clients, service_config):
    if (
        service_config.get_max_instances()
        and service_config.get_desired_state() == "start"
    ):
        all_mesos_tasks = a_sync.block(get_cached_list_of_running_tasks_from_frameworks)
        autoscaling_params = service_config.get_autoscaling_params()
        autoscaling_params.update({"noop": True})
        system_paasta_config = load_system_paasta_config()
        try:
            marathon_tasks, mesos_tasks = filter_autoscaling_tasks(
                [app for (app, client) in apps_with_clients],
                all_mesos_tasks,
                service_config,
                system_paasta_config,
            )
            utilization = get_utilization(
                marathon_service_config=service_config,
                system_paasta_config=system_paasta_config,
                autoscaling_params=autoscaling_params,
                log_utilization_data={},
                marathon_tasks=list(marathon_tasks.values()),
                mesos_tasks=mesos_tasks,
            )
            error = get_error_from_utilization(
                utilization=utilization,
                setpoint=autoscaling_params["setpoint"],
                current_instances=service_config.get_instances(),
            )
            new_instance_count = get_new_instance_count(
                utilization=utilization,
                error=error,
                autoscaling_params=autoscaling_params,
                current_instances=service_config.get_instances(),
                marathon_service_config=service_config,
                num_healthy_instances=len(marathon_tasks),
                persist_data=False,
            )
        except MetricsProviderNoDataError:
            utilization = None
            new_instance_count = None
        return ServiceAutoscalingInfo(
            current_instances=service_config.get_instances(),
            max_instances=service_config.get_max_instances(),
            min_instances=service_config.get_min_instances(),
            current_utilization=utilization,
            target_instances=new_instance_count,
        )
    return None


def get_new_instance_count(
    utilization,
    error,
    autoscaling_params,
    current_instances,
    marathon_service_config,
    num_healthy_instances,
    persist_data: bool,
):
    autoscaling_decision_policy = get_decision_policy(
        autoscaling_params[DECISION_POLICY_KEY]
    )

    zookeeper_path = compose_autoscaling_zookeeper_root(
        service=marathon_service_config.service,
        instance=marathon_service_config.instance,
    )
    autoscaling_amount = autoscaling_decision_policy(
        utilization=utilization,
        error=error,
        current_instances=current_instances,
        zookeeper_path=zookeeper_path,
        num_healthy_instances=num_healthy_instances,
        persist_data=persist_data,
        **autoscaling_params,
    )

    # Limit downscaling by 30% of current_instances until we find out what is
    # going on in such situations
    safe_downscaling_threshold = int(current_instances * 0.7)
    new_instance_count = max(
        current_instances + autoscaling_amount, safe_downscaling_threshold
    )

    new_instance_count = marathon_service_config.limit_instance_count(
        new_instance_count
    )
    return new_instance_count


def get_utilization(
    marathon_service_config,
    system_paasta_config,
    autoscaling_params,
    log_utilization_data,
    marathon_tasks,
    mesos_tasks,
):
    autoscaling_metrics_provider = get_service_metrics_provider(
        autoscaling_params[SERVICE_METRICS_PROVIDER_KEY]
    )

    return autoscaling_metrics_provider(
        marathon_service_config=marathon_service_config,
        system_paasta_config=system_paasta_config,
        marathon_tasks=marathon_tasks,
        mesos_tasks=mesos_tasks,
        log_utilization_data=log_utilization_data,
        **autoscaling_params,
    )


def is_task_data_insufficient(
    marathon_service_config, marathon_tasks, current_instances
):
    return len(marathon_tasks) < int((1 - MAX_TASK_DELTA) * current_instances)


def autoscale_marathon_instance(
    marathon_service_config: MarathonServiceConfig,
    system_paasta_config: SystemPaastaConfig,
    marathon_tasks: Sequence[MarathonTask],
    mesos_tasks: Sequence[Task],
) -> None:
    try:
        with create_autoscaling_lock(
            marathon_service_config.service, marathon_service_config.instance
        ):
            current_instances = marathon_service_config.get_instances()
            task_data_insufficient = is_task_data_insufficient(
                marathon_service_config=marathon_service_config,
                marathon_tasks=marathon_tasks,
                current_instances=current_instances,
            )
            autoscaling_params = marathon_service_config.get_autoscaling_params()
            log_utilization_data: Mapping = {}
            utilization = get_utilization(
                marathon_service_config=marathon_service_config,
                system_paasta_config=system_paasta_config,
                autoscaling_params=autoscaling_params,
                log_utilization_data=log_utilization_data,
                marathon_tasks=marathon_tasks,
                mesos_tasks=mesos_tasks,
            )
            error = get_error_from_utilization(
                utilization=utilization,
                setpoint=autoscaling_params["setpoint"],
                current_instances=current_instances,
            )
            num_healthy_instances = len(marathon_tasks)
            new_instance_count = get_new_instance_count(
                utilization=utilization,
                error=error,
                autoscaling_params=autoscaling_params,
                current_instances=current_instances,
                marathon_service_config=marathon_service_config,
                num_healthy_instances=num_healthy_instances,
                persist_data=(not task_data_insufficient),
            )
            safe_downscaling_threshold = int(current_instances * 0.7)
            _record_autoscaling_decision(
                marathon_service_config=marathon_service_config,
                autoscaling_params=autoscaling_params,
                utilization=utilization,
                log_utilization_data=log_utilization_data,
                error=error,
                current_instances=current_instances,
                num_healthy_instances=num_healthy_instances,
                new_instance_count=new_instance_count,
                safe_downscaling_threshold=safe_downscaling_threshold,
                task_data_insufficient=task_data_insufficient,
            )
            if new_instance_count != current_instances:
                if new_instance_count < current_instances and task_data_insufficient:
                    write_to_log(
                        config=marathon_service_config,
                        line="Delaying scaling *down* as we found too few healthy tasks running in marathon. "
                        "This can happen because tasks are delayed/waiting/unhealthy or because we are "
                        "waiting for tasks to be killed. Will wait for sufficient healthy tasks before "
                        "we make a decision to scale down.",
                        level="debug",
                    )
                    return
                else:
                    set_instances_for_marathon_service(
                        service=marathon_service_config.service,
                        instance=marathon_service_config.instance,
                        instance_count=new_instance_count,
                    )
                    write_to_log(
                        config=marathon_service_config,
                        line="Scaling from %d to %d instances (%s)"
                        % (
                            current_instances,
                            new_instance_count,
                            humanize_error(error),
                        ),
                        level="event",
                    )
            else:
                write_to_log(
                    config=marathon_service_config,
                    line="Staying at %d instances (%s)"
                    % (current_instances, humanize_error(error)),
                    level="debug",
                )
    except LockHeldException:
        log.warning(
            "Skipping autoscaling run for {service}.{instance} because the lock is held".format(
                service=marathon_service_config.service,
                instance=marathon_service_config.instance,
            )
        )


def _record_autoscaling_decision(
    marathon_service_config: MarathonServiceConfig,
    autoscaling_params: AutoscalingParamsDict,
    utilization: float,
    log_utilization_data: Mapping[str, str],
    error: float,
    current_instances: int,
    num_healthy_instances: int,
    new_instance_count: int,
    safe_downscaling_threshold: int,
    task_data_insufficient: bool,
) -> None:
    """
    Based on the calculations made, perform observability side effects.
    Log messages, generate time series, send any alerts, etc.
    """
    write_to_log(
        config=marathon_service_config,
        line=json.dumps(
            dict(
                timestamp=time.time(),
                paasta_cluster=marathon_service_config.get_cluster(),
                paasta_service=marathon_service_config.get_service(),
                paasta_instance=marathon_service_config.get_instance(),
                autoscaling_params=autoscaling_params,
                utilization=utilization,
                error=error,
                current_instances=current_instances,
                num_healthy_instances=num_healthy_instances,
                new_instance_count=new_instance_count,
                safe_downscaling_threshold=safe_downscaling_threshold,
                task_data_insufficient=task_data_insufficient,
            )
        ),
        level="debug",
    )
    meteorite_dims = {
        "paasta_service": marathon_service_config.service,
        "paasta_cluster": marathon_service_config.cluster,
        "paasta_instance": marathon_service_config.instance,
        "paasta_pool": marathon_service_config.get_pool(),
        "decision_policy": autoscaling_params[DECISION_POLICY_KEY],  # type: ignore
    }
    if yelp_meteorite:
        gauge = yelp_meteorite.create_gauge("paasta.service.instances", meteorite_dims)
        gauge.set(new_instance_count)
        gauge = yelp_meteorite.create_gauge(
            "paasta.service.max_instances", meteorite_dims
        )
        gauge.set(marathon_service_config.get_max_instances())
        gauge = yelp_meteorite.create_gauge(
            "paasta.service.min_instances", meteorite_dims
        )
        gauge.set(marathon_service_config.get_min_instances())


def humanize_error(error):
    if error < 0:
        return "%d%% underutilized" % floor(-error * 100)
    elif error > 0:
        return "%d%% overutilized" % ceil(error * 100)
    else:
        return "utilization within thresholds"


def get_configs_of_services_to_scale(
    cluster: str,
    soa_dir: str = DEFAULT_SOA_DIR,
    services: Optional[Sequence[str]] = None,
) -> Sequence[MarathonServiceConfig]:
    if not services:
        services = list_services(soa_dir=soa_dir)
    configs = []
    for service in services:
        service_config = PaastaServiceConfigLoader(service=service, soa_dir=soa_dir)
        for instance_config in service_config.instance_configs(
            cluster=cluster, instance_type_class=MarathonServiceConfig
        ):
            if (
                instance_config.get_max_instances()
                and instance_config.get_desired_state() == "start"
                and instance_config.get_autoscaling_params()["decision_policy"]
                != "bespoke"
            ):
                configs.append(instance_config)

    return configs


def autoscaling_is_paused():
    with ZookeeperPool() as zk:
        try:
            pause_until = zk.get(ZK_PAUSE_AUTOSCALE_PATH)[0].decode("utf8")
            pause_until = float(pause_until)
        except (NoNodeError, ValueError, AttributeError):
            pause_until = 0

    remaining = pause_until - time.time()
    if remaining >= 0:
        log.debug("Autoscaling is paused for {} more seconds".format(str(remaining)))
        return True
    else:
        return False


def autoscale_services(
    soa_dir: str = DEFAULT_SOA_DIR, services: Optional[Sequence[str]] = None
) -> None:
    system_paasta_config = load_system_paasta_config()
    cluster = system_paasta_config.get_cluster()
    configs = get_configs_of_services_to_scale(
        cluster=cluster, soa_dir=soa_dir, services=services
    )
    autoscale_service_configs(
        service_configs=configs, system_paasta_config=system_paasta_config
    )


@use_requests_cache("service_autoscaler")
def autoscale_service_configs(
    service_configs: Sequence[MarathonServiceConfig],
    system_paasta_config: SystemPaastaConfig,
) -> None:
    if autoscaling_is_paused():
        log.warning("Skipping autoscaling because autoscaler paused")
        return

    marathon_clients = get_marathon_clients(get_marathon_servers(system_paasta_config))
    apps_with_clients = get_marathon_apps_with_clients(
        marathon_clients.get_all_clients(), embed_tasks=True
    )
    all_mesos_tasks = a_sync.block(get_all_running_tasks)
    with ZookeeperPool():
        for config in service_configs:
            try:
                marathon_tasks, mesos_tasks = filter_autoscaling_tasks(
                    [app for (app, client) in apps_with_clients],
                    all_mesos_tasks,
                    config,
                    system_paasta_config,
                )
                autoscale_marathon_instance(
                    config,
                    system_paasta_config,
                    list(marathon_tasks.values()),
                    mesos_tasks,
                )
            except Exception as e:
                write_to_log(
                    config=config, line="Caught Exception %s" % e, level="debug"
                )


def filter_autoscaling_tasks(
    marathon_apps: Sequence[MarathonApp],
    all_mesos_tasks: Sequence[Task],
    config: MarathonServiceConfig,
    system_paasta_config: SystemPaastaConfig,
) -> Tuple[Mapping[str, MarathonTask], Sequence[Task]]:
    """Find the tasks that are serving traffic. We care about this because many tasks have a period of high CPU when
    they first start up, during which they warm up code, load and process data, etc., and we don't want this high load
    to drag our overall load estimate upwards. Allowing these tasks to count towards overall load could cause a cycle of
    scaling up, seeing high load due to new warming-up containers, scaling up, until we hit max_instances.

    However, accidentally omitting a task that actually is serving traffic will cause us to underestimate load; this is
    generally much worse than overestimating, since it can cause us to incorrectly scale down or refuse to scale up when
    necessary. For this reason, we look at several sources of health information, and if they disagree, assume the task
    is serving traffic.
    """
    job_id_prefix = "{}{}".format(
        format_job_id(service=config.service, instance=config.instance),
        MESOS_TASK_SPACER,
    )

    # Get a dict of healthy tasks, we assume tasks with no healthcheck defined are healthy.
    # We assume tasks with no healthcheck results but a defined healthcheck to be unhealthy, unless they are "old" in
    # which case we assume that Marathon has screwed up and stopped healthchecking but that they are healthy.

    log.info("Inspecting %s for autoscaling" % job_id_prefix)

    relevant_tasks_by_app: Dict[MarathonApp, List[MarathonTask]] = {
        app: app.tasks
        for app in marathon_apps
        if app.id.lstrip("/").startswith(job_id_prefix)
    }

    healthy_marathon_tasks: Dict[str, MarathonTask] = {}

    for app, tasks in relevant_tasks_by_app.items():
        for task in tasks:
            if (
                is_task_healthy(task)
                or not app.health_checks
                or is_old_task_missing_healthchecks(task, app)
            ):
                healthy_marathon_tasks[task.id] = task

    service_namespace_config = load_service_namespace_config(
        service=config.service, namespace=config.get_nerve_namespace()
    )
    if service_namespace_config.is_in_smartstack():

        for task in filter_tasks_in_smartstack(
            tasks=[task for tasks in relevant_tasks_by_app.values() for task in tasks],
            service=config.service,
            nerve_ns=config.get_nerve_namespace(),
            system_paasta_config=system_paasta_config,
            max_hosts_to_query=20,
            haproxy_min_fraction_up=0.01,  # Be very liberal. See docstring above for rationale.
        ):
            healthy_marathon_tasks[task.id] = task

    if not healthy_marathon_tasks:
        raise MetricsProviderNoDataError("Couldn't find any healthy marathon tasks")
    mesos_tasks = [
        task for task in all_mesos_tasks if task["id"] in healthy_marathon_tasks
    ]
    return (healthy_marathon_tasks, mesos_tasks)


def write_to_log(config, line, level="event"):
    _log(
        service=config.service,
        line=line,
        component="deploy",
        level=level,
        cluster=config.cluster,
        instance=config.instance,
    )


def get_short_job_id(task_id):
    return MESOS_TASK_SPACER.join(task_id.split(MESOS_TASK_SPACER, 2)[:2])


@contextmanager
def create_autoscaling_lock(service: str, instance: str) -> Iterator[None]:
    """Acquire a lock in zookeeper for autoscaling. This is
    to avoid autoscaling a service multiple times, and to avoid
    having multiple paasta services all attempting to autoscale and
    fetching mesos data."""
    zk = KazooClient(
        hosts=load_system_paasta_config().get_zk_hosts(),
        timeout=ZK_LOCK_CONNECT_TIMEOUT_S,
    )
    zk.start()
    lock = zk.Lock(f"/autoscaling/{service}/{instance}/autoscaling.lock")
    try:
        lock.acquire(timeout=1)  # timeout=0 throws some other strange exception
        yield
    except LockTimeout:
        raise LockHeldException(
            f"Failed to acquire lock for autoscaling! {service}.{instance}"
        )
    else:
        lock.release()
    finally:
        zk.stop()
        zk.close()
