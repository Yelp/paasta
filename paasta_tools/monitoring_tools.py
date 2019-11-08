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
import json
import logging
import os
from typing import Optional

import pysensu_yelp
import service_configuration_lib

from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import is_under_replicated
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaNotConfiguredError


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


def __get_monitoring_config_value(
    key,
    overrides,
    service,
    soa_dir=DEFAULT_SOA_DIR,
    monitoring_defaults=monitoring_defaults,
):
    general_config = service_configuration_lib.read_service_configuration(
        service, soa_dir=soa_dir
    )
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
    service, check_name, overrides, status, output, soa_dir, ttl=None, cluster=None
):
    """Send an event to sensu via pysensu_yelp with the given information.

    :param service: The service name the event is about
    :param check_name: The name of the check as it appears in Sensu
    :param overrides: A dictionary containing overrides for monitoring options
                      (e.g. notification_email, ticket, page)
    :param status: The status to emit for this event
    :param output: The output to emit for this event
    :param soa_dir: The service directory to read monitoring information from
    :param cluster: The cluster name (optional)
    """
    # This function assumes the input is a string like "mumble.main"
    team = get_team(overrides, service, soa_dir)
    if not team:
        return

    system_paasta_config = load_system_paasta_config()
    if cluster is None:
        try:
            cluster = system_paasta_config.get_cluster()
        except PaastaNotConfiguredError:
            cluster = "localhost"

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
        "alert_after": overrides.get("alert_after", "5m"),
        "irc_channels": get_irc_channels(overrides, service, soa_dir),
        "slack_channels": get_slack_channels(overrides, service, soa_dir),
        "ticket": get_ticket(overrides, service, soa_dir),
        "project": get_project(overrides, service, soa_dir),
        "priority": get_priority(overrides, service, soa_dir),
        "source": f"paasta-{cluster}",
        "tags": get_tags(overrides, service, soa_dir),
        "ttl": ttl,
        "sensu_host": system_paasta_config.get_sensu_host(),
        "sensu_port": system_paasta_config.get_sensu_port(),
        "component": get_component(overrides, service, soa_dir),
        "description": get_description(overrides, service, soa_dir),
    }

    if result_dict.get("sensu_host"):
        pysensu_yelp.send_event(**result_dict)


def read_monitoring_config(service, soa_dir=DEFAULT_SOA_DIR):
    """Read a service's monitoring.yaml file.

    :param service: The service name
    :param soa_dir: THe SOA configuration directory to read from
    :returns: A dictionary of whatever was in soa_dir/name/monitoring.yaml"""
    rootdir = os.path.abspath(soa_dir)
    monitoring_file = os.path.join(rootdir, service, "monitoring.yaml")
    monitor_conf = service_configuration_lib.read_monitoring(monitoring_file)
    return monitor_conf


def list_teams(**kwargs):
    """Loads team data from the system. Returns a set of team names (or empty
    set).
    """
    team_data = _load_sensu_team_data()
    teams = set(team_data.get("team_data", {}).keys())
    return teams


def send_replication_event(instance_config, status, output):
    """Send an event to sensu via pysensu_yelp with the given information.

    :param instance_config: an instance of LongRunningServiceConfig
    :param status: The status to emit for this event
    :param output: The output to emit for this event"""
    # This function assumes the input is a string like "mumble.main"
    monitoring_overrides = instance_config.get_monitoring()
    if "alert_after" not in monitoring_overrides:
        monitoring_overrides["alert_after"] = "2m"
    monitoring_overrides["check_every"] = "1m"
    monitoring_overrides["runbook"] = get_runbook(
        monitoring_overrides, instance_config.service, soa_dir=instance_config.soa_dir
    )

    check_name = f"check_paasta_services_replication.{instance_config.job_id}"
    send_event(
        service=instance_config.service,
        check_name=check_name,
        overrides=monitoring_overrides,
        status=status,
        output=output,
        soa_dir=instance_config.soa_dir,
        cluster=instance_config.cluster,
    )
    _log(
        service=instance_config.service,
        line=f"Replication: {output}",
        component="monitoring",
        level="debug",
        cluster=instance_config.cluster,
        instance=instance_config.instance,
    )


def check_smartstack_replication_for_instance(
    instance_config, expected_count, smartstack_replication_checker
):
    """Check a set of namespaces to see if their number of available backends is too low,
    emitting events to Sensu based on the fraction available and the thresholds defined in
    the corresponding yelpsoa config.

    :param instance_config: an instance of MarathonServiceConfig
    :param smartstack_replication_checker: an instance of SmartstackReplicationChecker
    """

    crit_threshold = instance_config.get_replication_crit_percentage()

    log.info("Checking instance %s in smartstack", instance_config.job_id)
    smartstack_replication_info = smartstack_replication_checker.get_replication_for_instance(
        instance_config
    )

    log.debug(
        "Got smartstack replication info for %s: %s"
        % (instance_config.job_id, smartstack_replication_info)
    )

    if len(smartstack_replication_info) == 0:
        status = pysensu_yelp.Status.CRITICAL
        output = (
            "Service %s has no Smartstack replication info. Make sure the discover key in your smartstack.yaml "
            "is valid!\n"
        ) % instance_config.job_id
        log.error(output)
    else:
        expected_count_per_location = int(
            expected_count / len(smartstack_replication_info)
        )
        output = ""
        output_critical = ""
        output_ok = ""
        under_replication_per_location = []

        for location, available_backends in sorted(smartstack_replication_info.items()):
            num_available_in_location = available_backends.get(
                instance_config.job_id, 0
            )
            under_replicated, ratio = is_under_replicated(
                num_available_in_location, expected_count_per_location, crit_threshold
            )
            if under_replicated:
                output_critical += (
                    "- Service %s has %d out of %d expected instances in %s (CRITICAL: %d%%)\n"
                    % (
                        instance_config.job_id,
                        num_available_in_location,
                        expected_count_per_location,
                        location,
                        ratio,
                    )
                )
            else:
                output_ok += (
                    "- Service %s has %d out of %d expected instances in %s (OK: %d%%)\n"
                    % (
                        instance_config.job_id,
                        num_available_in_location,
                        expected_count_per_location,
                        location,
                        ratio,
                    )
                )
            under_replication_per_location.append(under_replicated)

        output += output_critical
        if output_critical and output_ok:
            output += "\n\n"
            output += "The following locations are OK:\n"
        output += output_ok

        if any(under_replication_per_location):
            status = pysensu_yelp.Status.CRITICAL
            output += (
                "\n\n"
                "What this alert means:\n"
                "\n"
                "  This replication alert means that a SmartStack powered loadbalancer (haproxy)\n"
                "  doesn't have enough healthy backends. Not having enough healthy backends\n"
                "  means that clients of that service will get 503s (http) or connection refused\n"
                "  (tcp) when trying to connect to it.\n"
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
                "      paasta logs -s %(service)s -i %(instance)s -c %(cluster)s\n"
                "\n"
                "  * Fix the cause of the unhealthy service. Try running:\n"
                "\n"
                "      paasta status -s %(service)s -i %(instance)s -c %(cluster)s -vv\n"
                "\n"
                "  * Widen SmartStack discovery settings\n"
                "  * Increase the instance count\n"
                "\n"
            ) % {
                "service": instance_config.service,
                "instance": instance_config.instance,
                "cluster": instance_config.cluster,
            }
            log.error(output)
        else:
            status = pysensu_yelp.Status.OK
            log.info(output)
    send_replication_event(
        instance_config=instance_config, status=status, output=output
    )


def send_replication_event_if_under_replication(
    instance_config,
    expected_count: int,
    num_available: int,
    sub_component: Optional[str] = None,
):
    crit_threshold = instance_config.get_replication_crit_percentage()
    if sub_component is not None:
        output = (
            "Service %s has %d out of %d expected instances of %s available!\n"
            + "(threshold: %d%%)"
        ) % (
            instance_config.job_id,
            num_available,
            expected_count,
            sub_component,
            crit_threshold,
        )
    else:
        output = (
            "Service %s has %d out of %d expected instances available!\n"
            + "(threshold: %d%%)"
        ) % (instance_config.job_id, num_available, expected_count, crit_threshold)
    under_replicated, _ = is_under_replicated(
        num_available, expected_count, crit_threshold
    )
    if under_replicated:
        output += (
            "\n\n"
            "What this alert means:\n"
            "\n"
            "  This replication alert means that the service PaaSTA can't keep the\n"
            "  requested number of copies up and healthy in the cluster.\n"
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
            "      paasta status -s %(service)s -i %(instance)s -c %(cluster)s -vv\n"
        ) % {
            "service": instance_config.service,
            "instance": instance_config.instance,
            "cluster": instance_config.cluster,
        }
        log.error(output)
        status = pysensu_yelp.Status.CRITICAL
    else:
        log.info(output)
        status = pysensu_yelp.Status.OK
    send_replication_event(
        instance_config=instance_config, status=status, output=output
    )
