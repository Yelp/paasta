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
import argparse
import logging
import sys
import traceback
from socket import getfqdn
from socket import gethostbyname
from socket import gethostname

from paasta_tools import mesos_maintenance
from paasta_tools import utils
from paasta_tools.marathon_tools import get_expected_instance_count_for_namespace
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.marathon_tools import marathon_services_running_here
from paasta_tools.smartstack_tools import backend_is_up
from paasta_tools.smartstack_tools import get_backends
from paasta_tools.smartstack_tools import get_replication_for_services
from paasta_tools.smartstack_tools import ip_port_hostname_from_svname
from paasta_tools.smartstack_tools import load_smartstack_info_for_service

log = logging.getLogger(__name__)


def parse_args():
    """Parses the command line arguments passed to this script"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--duration",
        type=mesos_maintenance.parse_timedelta,
        default="1h",
        help="Duration of the maintenance window. Any pytimeparse unit is supported.",
    )
    parser.add_argument(
        "-s",
        "--start",
        type=mesos_maintenance.parse_datetime,
        default=str(mesos_maintenance.now()),
        help="Time to start the maintenance window. Defaults to now.",
    )
    parser.add_argument(
        "action",
        choices=[
            "cluster_status",
            "down",
            "drain",
            "is_host_down",
            "is_host_drained",
            "is_host_draining",
            "is_hosts_past_maintenance_end",
            "is_hosts_past_maintenance_start",
            "is_safe_to_drain",
            "is_safe_to_kill",
            "schedule",
            "status",
            "undrain",
            "up",
        ],
        help="Action to perform on the specified hosts",
    )
    parser.add_argument(
        "hostname",
        nargs="*",
        default=[getfqdn()],
        help="Hostname(s) of machine(s) to start draining. "
        "You can specify <hostname>|<ip> to avoid querying DNS to determine the corresponding IP.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        dest="verbose",
        default=0,
        help="Print out more output.",
    )
    return parser.parse_args()


def is_safe_to_kill(hostname):
    """Checks if a host has drained or reached its maintenance window
    :param hostname: hostname to check
    :returns: True or False
    """
    return mesos_maintenance.is_host_drained(
        hostname
    ) or mesos_maintenance.is_host_past_maintenance_start(hostname)


def is_hostname_local(hostname):
    return hostname == "localhost" or hostname == getfqdn() or hostname == gethostname()


def is_safe_to_drain(hostname):
    """Checks if a host has healthy tasks running locally that have low
    replication in other places
    :param hostname: hostname to check
    :returns: True or False
    """
    if not is_hostname_local(hostname):
        print(
            "Due to the way is_safe_to_drain is implemented, it can only work on localhost."
        )
        return False
    return not are_local_tasks_in_danger()


def is_healthy_in_haproxy(local_port, backends):
    local_ip = gethostbyname(gethostname())
    for backend in backends:
        ip, port, _ = ip_port_hostname_from_svname(backend["svname"])
        if ip == local_ip and port == local_port:
            if backend_is_up(backend):
                log.debug("Found a healthy local backend: %s" % backend)
                return True
            else:
                log.debug("Found a unhealthy local backend: %s" % backend)
                return False
    log.debug("Couldn't find any haproxy backend listening on %s" % local_port)
    return False


def synapse_replication_is_low(service, instance, system_paasta_config, local_backends):
    crit_threshold = 80
    cluster = system_paasta_config.get_cluster()
    marathon_service_config = load_marathon_service_config(
        service=service, instance=instance, cluster=cluster, load_deployments=False
    )
    reg_svc, reg_namespace, _, __ = utils.decompose_job_id(
        marathon_service_config.get_registrations()
    )
    # We only actually care about the replication of where we're registering
    service, namespace = reg_svc, reg_namespace

    smartstack_replication_info = load_smartstack_info_for_service(
        service=service,
        namespace=namespace,
        blacklist=[],
        system_paasta_config=system_paasta_config,
    )
    expected_count = get_expected_instance_count_for_namespace(
        service=service, namespace=namespace
    )
    expected_count_per_location = int(expected_count / len(smartstack_replication_info))

    synapse_name = utils.compose_job_id(service, namespace)
    local_replication = get_replication_for_services(
        synapse_host=system_paasta_config.get_default_synapse_host(),
        synapse_port=system_paasta_config.get_synapse_port(),
        synapse_haproxy_url_format=system_paasta_config.get_synapse_haproxy_url_format(),
        services=[synapse_name],
    )
    num_available = local_replication.get(synapse_name, 0)
    under_replicated, ratio = utils.is_under_replicated(
        num_available, expected_count_per_location, crit_threshold
    )
    log.info(
        "Service %s.%s has %d out of %d expected instances"
        % (service, instance, num_available, expected_count_per_location)
    )
    return under_replicated


def are_local_tasks_in_danger():
    try:
        system_paasta_config = utils.load_system_paasta_config()
        local_services = marathon_services_running_here()
        local_backends = get_backends(
            service=None,
            synapse_host=system_paasta_config.get_default_synapse_host(),
            synapse_port=system_paasta_config.get_synapse_port(),
            synapse_haproxy_url_format=system_paasta_config.get_synapse_haproxy_url_format(),
        )
        for service, instance, port in local_services:
            log.info(f"Inspecting {service}.{instance} on {port}")
            if is_healthy_in_haproxy(
                port, local_backends
            ) and synapse_replication_is_low(
                service, instance, system_paasta_config, local_backends=local_backends
            ):
                log.warning(
                    "{}.{} on port {} is healthy but the service is in danger!".format(
                        service, instance, port
                    )
                )
                return True
        return False
    except Exception:
        log.warning(traceback.format_exc())
        return False


def paasta_maintenance():
    """Manipulate the maintenance state of a PaaSTA host.
    :returns: None
    """
    args = parse_args()

    if args.verbose >= 2:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose == 1:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)

    action = args.action
    hostnames = args.hostname

    if action != "status" and not hostnames:
        print("You must specify one or more hostnames")
        return

    start = args.start
    duration = args.duration

    ret = "Done"
    if action == "drain":
        mesos_maintenance.drain(hostnames, start, duration)
    elif action == "undrain":
        mesos_maintenance.undrain(hostnames)
    elif action == "down":
        mesos_maintenance.down(hostnames)
    elif action == "up":
        mesos_maintenance.up(hostnames)
    elif action == "status":
        ret = mesos_maintenance.friendly_status()
    elif action == "cluster_status":
        ret = mesos_maintenance.status()
    elif action == "schedule":
        ret = mesos_maintenance.schedule()
    elif action == "is_safe_to_drain":
        ret = is_safe_to_drain(hostnames[0])
    elif action == "is_safe_to_kill":
        ret = is_safe_to_kill(hostnames[0])
    elif action == "is_host_drained":
        ret = mesos_maintenance.is_host_drained(hostnames[0])
    elif action == "is_host_down":
        ret = mesos_maintenance.is_host_down(hostnames[0])
    elif action == "is_host_draining":
        ret = mesos_maintenance.is_host_draining(hostnames[0])
    elif action == "is_host_past_maintenance_start":
        ret = mesos_maintenance.is_host_past_maintenance_start(hostnames[0])
    elif action == "is_host_past_maintenance_end":
        ret = mesos_maintenance.is_host_past_maintenance_end(hostnames[0])
    else:
        raise NotImplementedError("Action: '%s' is not implemented." % action)
    print(ret)
    return ret


if __name__ == "__main__":
    if paasta_maintenance():
        sys.exit(0)
    sys.exit(1)
