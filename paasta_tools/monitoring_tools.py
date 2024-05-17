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
Getters for deriving monitoring parameters for mesos-deployed stuff.
This leaves a place for sane defaults that might change depending
on the framework that is asking, and still allows you to set your team
*once* for a service in the general config.

Everything in here is private, and you shouldn't worry about it.
"""
import abc
import json
import logging
import os
from typing import Dict
from typing import Mapping
from typing import Optional
from typing import Tuple

import pysensu_yelp
import service_configuration_lib

from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import is_under_replicated
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import time_cache


class ReplicationChecker(abc.ABC):
    @abc.abstractmethod
    def get_replication_for_instance(
        self, instance_config: LongRunningServiceConfig
    ) -> Dict[str, Dict[str, Dict[str, int]]]:
        ...


try:
    import yelp_meteorite
except ImportError:
    yelp_meteorite = None


DEFAULT_REPLICATION_RUNBOOK = "y/unhealthy-paasta-instances"

log = logging.getLogger(__name__)


def monitoring_defaults(key):
    defaults = {
        "runbook": 'Please set a `runbook` field in your monitoring.yaml. Like "y/rb-mesos". Docs: '
        "https://paasta.readthedocs.io/en/latest/yelpsoa_configs.html#monitoring-yaml",
        "tip": "Please set a `tip` field in your monitoring.yaml. Docs: "
        "https://paasta.readthedocs.io/en/latest/yelpsoa_configs.html#monitoring-yaml",
        "ticket": False,
        "project": None,
        "realert_every": -1,
        "tags": [],
    }
    return defaults.get(key, None)


def get_team(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value("team", overrides, service, soa_dir)


def get_runbook(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value("runbook", overrides, service, soa_dir)


def get_tip(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value("tip", overrides, service, soa_dir)


def get_notification_email(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value(
        "notification_email", overrides, service, soa_dir
    )


def get_page(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value("page", overrides, service, soa_dir)


def get_alert_after(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value("alert_after", overrides, service, soa_dir)


def get_realert_every(
    overrides, service, soa_dir=DEFAULT_SOA_DIR, monitoring_defaults=monitoring_defaults
):
    return __get_monitoring_config_value(
        "realert_every",
        overrides=overrides,
        service=service,
        soa_dir=soa_dir,
        monitoring_defaults=monitoring_defaults,
    )


def get_check_every(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value("check_every", overrides, service, soa_dir)


def get_irc_channels(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value("irc_channels", overrides, service, soa_dir)


def get_slack_channels(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value("slack_channels", overrides, service, soa_dir)


def get_dependencies(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value("dependencies", overrides, service, soa_dir)


def get_ticket(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value("ticket", overrides, service, soa_dir)


def get_project(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value("project", overrides, service, soa_dir)


def get_priority(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value("priority", overrides, service, soa_dir)


def get_tags(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value("tags", overrides, service, soa_dir)


def get_component(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value("component", overrides, service, soa_dir)


def get_description(overrides, service, soa_dir=DEFAULT_SOA_DIR):
    return __get_monitoring_config_value("description", overrides, service, soa_dir)


# Our typical usage pattern is that we call all the different get_* functions back to back. Applying a small amount of
# cache here helps cut down on the number of times we re-parse service.yaml.
_cached_read_service_configuration = time_cache(ttl=5)(
    service_configuration_lib.read_service_configuration
)


def __get_monitoring_config_value(
    key,
    overrides,
    service,
    soa_dir=DEFAULT_SOA_DIR,
    monitoring_defaults=monitoring_defaults,
):
    general_config = _cached_read_service_configuration(service, soa_dir=soa_dir)
    monitor_config = read_monitoring_config(service, soa_dir=soa_dir)
    service_default = general_config.get(key, monitoring_defaults(key))
    service_default = general_config.get("monitoring", {key: service_default}).get(
        key, service_default
    )
    service_default = monitor_config.get(key, service_default)
    return overrides.get(key, service_default)


def get_sensu_team_data(team):
    """Takes a team and returns the dictionary of Sensu configuration
    settings for that team. The data is in this format:
    https://github.com/Yelp/sensu_handlers#teams
    Returns an empty dictionary if there is nothing to return.

    Not all teams specify all the different types of configuration settings.
    for example, a team may not specify a `notification_email`. It is up
    to the caller of this function to handle that case.
    """
    global_team_data = _load_sensu_team_data()["team_data"]
    return global_team_data.get(team, {})


def _load_sensu_team_data():
    try:
        with open("/etc/sensu/team_data.json") as f:
            team_data = json.load(f)
    except IOError:
        log.warning(
            "No Sensu Team data (/etc/sensu/team_data.json) available. Using empty defaults"
        )
        team_data = {}
    return team_data


def send_event(
    service,
    check_name,
    overrides,
    status,
    output,
    soa_dir,
    ttl=None,
    cluster=None,
    system_paasta_config=None,
    dry_run=False,
):
    """Send an event to sensu via pysensu_yelp with the given information.

    :param service: The service name the event is about
    :param check_name: The name of the check as it appears in Sensu
    :param overrides: A dictionary containing overrides for monitoring options
                      (e.g. notification_email, ticket, page)
    :param status: The status to emit for this event
    :param output: The output to emit for this event
    :param soa_dir: The service directory to read monitoring information from
    :param ttl: TTL (optional)
    :param cluster: The cluster name (optional)
    :param system_paasta_config: A SystemPaastaConfig object representing the system
    :param dry_run: Print the Sensu event instead of emitting it
    """
    # This function assumes the input is a string like "mumble.main"
    team = get_team(overrides, service, soa_dir)
    if not team:
        return

    if system_paasta_config is None:
        system_paasta_config = load_system_paasta_config()
    if cluster is None:
        try:
            cluster = system_paasta_config.get_cluster()
        except PaastaNotConfiguredError:
            cluster = "localhost"

    alert_after = overrides.get("alert_after", "5m")
    result_dict = {
        "name": check_name,
        "runbook": overrides.get("runbook", "http://y/paasta-troubleshooting"),
        "status": status,
        "output": output,
        "team": team,
        "page": get_page(overrides, service, soa_dir),
        "tip": get_tip(overrides, service, soa_dir),
        "notification_email": get_notification_email(overrides, service, soa_dir),
        "check_every": overrides.get("check_every", "1m"),
        "realert_every": overrides.get(
            "realert_every", monitoring_defaults("realert_every")
        ),
        "alert_after": f"{alert_after}s"
        if isinstance(alert_after, int)
        else alert_after,
        "irc_channels": get_irc_channels(overrides, service, soa_dir),
        "slack_channels": get_slack_channels(overrides, service, soa_dir),
        "ticket": get_ticket(overrides, service, soa_dir),
        "project": get_project(overrides, service, soa_dir),
        "priority": get_priority(overrides, service, soa_dir),
        "source": "paasta-%s" % cluster,
        "tags": get_tags(overrides, service, soa_dir),
        "ttl": ttl,
        "sensu_host": system_paasta_config.get_sensu_host(),
        "sensu_port": system_paasta_config.get_sensu_port(),
        "component": get_component(overrides, service, soa_dir),
        "description": get_description(overrides, service, soa_dir),
    }

    if dry_run:
        if status == pysensu_yelp.Status.OK:
            print(f"Would've sent an OK event for check '{check_name}'")
        else:
            from pprint import pprint  # only import during testing

            print(f"Would've sent the following alert for check '{check_name}':")
            pprint(result_dict)

    elif result_dict.get("sensu_host"):
        pysensu_yelp.send_event(**result_dict)


@time_cache(ttl=5)
def read_monitoring_config(service, soa_dir=DEFAULT_SOA_DIR):
    """Read a service's monitoring.yaml file.

    :param service: The service name
    :param soa_dir: THe SOA configuration directory to read from
    :returns: A dictionary of whatever was in soa_dir/name/monitoring.yaml"""
    rootdir = os.path.abspath(soa_dir)
    monitoring_file = os.path.join(rootdir, service, "monitoring.yaml")
    monitor_conf = service_configuration_lib.read_monitoring(monitoring_file)
    return monitor_conf


def list_teams():
    """Loads team data from the system. Returns a set of team names (or empty
    set).
    """
    team_data = _load_sensu_team_data()
    teams = set(team_data.get("team_data", {}).keys())
    return teams


def send_replication_event(
    instance_config,
    status,
    output,
    description,
    dry_run=False,
):
    """Send an event to sensu via pysensu_yelp with the given information.

    :param instance_config: an instance of LongRunningServiceConfig
    :param status: The status to emit for this event
    :param output: The output to emit for this event
    :param dry_run: Print the event instead of emitting it
    """
    # This function assumes the input is a string like "mumble.main"
    monitoring_overrides = instance_config.get_monitoring()
    if "alert_after" not in monitoring_overrides:
        monitoring_overrides["alert_after"] = "2m"
    monitoring_overrides["check_every"] = "1m"
    monitoring_overrides["runbook"] = __get_monitoring_config_value(
        "runbook",
        monitoring_overrides,
        instance_config.service,
        soa_dir=instance_config.soa_dir,
        monitoring_defaults=lambda _: DEFAULT_REPLICATION_RUNBOOK,
    )
    monitoring_overrides["tip"] = __get_monitoring_config_value(
        "tip",
        monitoring_overrides,
        instance_config.service,
        soa_dir=instance_config.soa_dir,
        monitoring_defaults=lambda _: (
            f"Check the instance with: `paasta status -s {instance_config.service} "
            f"-i {instance_config.instance} -c {instance_config.cluster} -vv`"
        ),
    )
    monitoring_overrides["description"] = description

    check_name = "check_paasta_services_replication.%s" % instance_config.job_id
    send_event(
        service=instance_config.service,
        check_name=check_name,
        overrides=monitoring_overrides,
        status=status,
        output=output,
        soa_dir=instance_config.soa_dir,
        cluster=instance_config.cluster,
        dry_run=dry_run,
    )
    _log(
        service=instance_config.service,
        line="Replication: %s" % output,
        component="monitoring",
        level="debug",
        cluster=instance_config.cluster,
        instance=instance_config.instance,
    )


def emit_replication_metrics(
    replication_infos: Mapping[str, Mapping[str, Mapping[str, int]]],
    instance_config: LongRunningServiceConfig,
    expected_count: int,
    dry_run: bool = False,
) -> None:
    for provider, replication_info in replication_infos.items():
        meteorite_dims = {
            "paasta_service": instance_config.service,
            "paasta_cluster": instance_config.cluster,
            "paasta_instance": instance_config.instance,
            "paasta_pool": instance_config.get_pool(),
            "service_discovery_provider": provider,
        }

        num_available_backends = 0
        for available_backends in replication_info.values():
            num_available_backends += available_backends.get(instance_config.job_id, 0)
        available_backends_metric = "paasta.service.available_backends"
        if dry_run:
            print(
                f"Would've sent value {num_available_backends} for metric '{available_backends_metric}'"
            )
        else:
            available_backends_gauge = yelp_meteorite.create_gauge(
                available_backends_metric, meteorite_dims
            )
            available_backends_gauge.set(num_available_backends)

        critical_percentage = instance_config.get_replication_crit_percentage()
        num_critical_backends = critical_percentage * expected_count / 100.0
        critical_backends_metric = "paasta.service.critical_backends"
        if dry_run:
            print(
                f"Would've sent value {num_critical_backends} for metric '{critical_backends_metric}'"
            )
        else:
            critical_backends_gauge = yelp_meteorite.create_gauge(
                critical_backends_metric, meteorite_dims
            )
            critical_backends_gauge.set(num_critical_backends)

        expected_backends_metric = "paasta.service.expected_backends"
        if dry_run:
            print(
                f"Would've sent value {expected_count} for metric '{expected_backends_metric}'"
            )
        else:
            expected_backends_gauge = yelp_meteorite.create_gauge(
                "paasta.service.expected_backends", meteorite_dims
            )
            expected_backends_gauge.set(expected_count)


def check_replication_for_instance(
    instance_config: LongRunningServiceConfig,
    expected_count: int,
    replication_checker: ReplicationChecker,
    dry_run: bool = False,
) -> bool:
    """Check a set of namespaces to see if their number of available backends is too low,
    emitting events to Sensu based on the fraction available and the thresholds defined in
    the corresponding yelpsoa config.

    :param instance_config: an instance of LongRunningServiceConfig
    :param replication_checker: an instance of ReplicationChecker
    :param dry_run: Print Sensu event and metrics instead of emitting them
    """

    crit_threshold = instance_config.get_replication_crit_percentage()

    log.info(
        "Checking instance %s in service discovery providers", instance_config.job_id
    )
    replication_infos = replication_checker.get_replication_for_instance(
        instance_config
    )

    log.debug(f"Got replication info for {instance_config.job_id}: {replication_infos}")
    if yelp_meteorite is not None:
        emit_replication_metrics(
            replication_infos,
            instance_config,
            expected_count,
            dry_run=dry_run,
        )

    service_is_under_replicated = False
    failed_service_discovery_providers = set()
    for service_discovery_provider, replication_info in replication_infos.items():
        if len(replication_info) == 0:
            output = (
                "Service %s has no %s replication info. Make sure the discover key in the corresponding config (e.g. smartstack.yaml for Smartstack) is valid!\n"
            ) % (instance_config.job_id, service_discovery_provider)
            log.error(output)
            service_is_under_replicated = True
            failed_service_discovery_providers.add(service_discovery_provider)
        else:
            expected_count_per_location = int(expected_count / len(replication_info))
            output_critical = []
            output_ok = []
            under_replication_per_location = []

            for location, available_backends in sorted(replication_info.items()):
                num_available_in_location = available_backends.get(
                    instance_config.job_id, 0
                )
                under_replicated, ratio = is_under_replicated(
                    num_available_in_location,
                    expected_count_per_location,
                    crit_threshold,
                )
                if under_replicated:
                    output_critical.append(
                        "{} has {}/{} replicas in {} according to {} (CRITICAL: {}%)\n".format(
                            instance_config.job_id,
                            num_available_in_location,
                            expected_count_per_location,
                            location,
                            service_discovery_provider,
                            ratio,
                        )
                    )
                    failed_service_discovery_providers.add(service_discovery_provider)
                else:
                    output_ok.append(
                        "{} has {}/{} replicas in {} according to {} (OK: {}%)\n".format(
                            instance_config.job_id,
                            num_available_in_location,
                            expected_count_per_location,
                            location,
                            service_discovery_provider,
                            ratio,
                        )
                    )
                under_replication_per_location.append(under_replicated)

            output = ", ".join(output_critical)
            if output_critical and output_ok:
                output += ". The following locations are OK: "
            output += ", ".join(output_ok)

            service_is_under_replicated_anywhere = any(under_replication_per_location)
            service_is_under_replicated |= service_is_under_replicated_anywhere
            if service_is_under_replicated_anywhere:
                log.error(output)
            else:
                log.info(output)

    if service_is_under_replicated:
        failed_service_discovery_providers_list = ",".join(
            failed_service_discovery_providers
        )
        description = (
            "This replication alert means that a {service_discovery_provider} powered loadbalancer\n"
            "doesn't have enough healthy backends. Not having enough healthy backends\n"
            "means that clients of that service will get 503s (http) or connection refused\n"
            "(tcp) when trying to connect to it.\n"
            "\n"
            "Reasons this might be happening:\n"
            "\n"
            "  The service may simply not have enough copies or it could simply be\n"
            "  unhealthy in that location. There also may not be enough resources\n"
            "  in the cluster to support the requested instance count.\n"
            "\n"
            "Things you can do:\n"
            "\n"
            "  * You can view the logs for the job with:\n"
            "      paasta logs -s {service} -i {instance} -c {cluster}\n"
            "\n"
            "  * Fix the cause of the unhealthy service. Try running:\n"
            "\n"
            "      paasta status -s {service} -i {instance} -c {cluster} -vv\n"
            "\n"
            "  * Widen {service_discovery_provider} discovery settings\n"
            "  * Increase the instance count\n"
            "\n"
        ).format(
            service=instance_config.service,
            instance=instance_config.instance,
            cluster=instance_config.cluster,
            service_discovery_provider=failed_service_discovery_providers_list,
        )
        status = pysensu_yelp.Status.CRITICAL
    else:
        description = (
            "{} is well-replicated because it has over {}% of its "
            "expected replicas up."
        ).format(instance_config.job_id, crit_threshold)
        status = pysensu_yelp.Status.OK

    send_replication_event(
        instance_config=instance_config,
        status=status,
        output=output,
        description=description,
        dry_run=dry_run,
    )
    return not service_is_under_replicated


def check_under_replication(
    instance_config: LongRunningServiceConfig,
    expected_count: int,
    num_available: int,
    sub_component: Optional[str] = None,
) -> Tuple[bool, str, str]:
    """Check if a component/sub_component is under-replicated and returns both the result of the check in the form of a
    boolean and a human-readable text to be used in logging or monitoring events.
    """
    crit_threshold = instance_config.get_replication_crit_percentage()

    # Keep output short, with rest of context in description. This is because
    # by default, Slack-Sensu messages have a 400 char limit, incl. the output.
    # If it is too long, the runbook and tip won't show up.
    if sub_component is not None:
        output = ("{} has {}/{} replicas of {} available (threshold: {}%)").format(
            instance_config.job_id,
            num_available,
            expected_count,
            sub_component,
            crit_threshold,
        )
    else:
        output = ("{} has {}/{} replicas available (threshold: {}%)").format(
            instance_config.job_id, num_available, expected_count, crit_threshold
        )

    under_replicated, _ = is_under_replicated(
        num_available, expected_count, crit_threshold
    )
    if under_replicated:
        description = (
            "This replication alert means that PaaSTA can't keep the\n"
            "requested number of replicas up and healthy in the cluster for "
            "the instance {service}.{instance}.\n"
            "\n"
            "Reasons this might be happening:\n"
            "\n"
            "  The service may simply be unhealthy. There also may not be enough resources\n"
            "  in the cluster to support the requested instance count.\n"
            "\n"
            "Things you can do:\n"
            "\n"
            "  * Increase the instance count\n"
            "  * Fix the cause of the unhealthy service. Try running:\n"
            "\n"
            "      paasta status -s {service} -i {instance} -c {cluster} -vv\n"
        ).format(
            service=instance_config.service,
            instance=instance_config.instance,
            cluster=instance_config.cluster,
        )
    else:
        description = (
            "{} is well-replicated because it has over {}% of its "
            "expected replicas up."
        ).format(instance_config.job_id, crit_threshold)
    return under_replicated, output, description


def send_replication_event_if_under_replication(
    instance_config: LongRunningServiceConfig,
    expected_count: int,
    num_available: int,
    sub_component: Optional[str] = None,
    dry_run: bool = False,
):
    under_replicated, output, description = check_under_replication(
        instance_config, expected_count, num_available, sub_component
    )
    if under_replicated:
        log.error(output)
        status = pysensu_yelp.Status.CRITICAL
    else:
        log.info(output)
        status = pysensu_yelp.Status.OK
    send_replication_event(
        instance_config=instance_config,
        status=status,
        output=output,
        description=description,
        dry_run=dry_run,
    )
