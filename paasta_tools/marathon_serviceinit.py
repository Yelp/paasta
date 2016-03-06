#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
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
import datetime
import logging

import humanize
import isodate
import requests_cache

from paasta_tools import marathon_tools
from paasta_tools.mesos_tools import get_mesos_slaves_grouped_by_attribute
from paasta_tools.mesos_tools import get_running_tasks_from_active_frameworks
from paasta_tools.mesos_tools import status_mesos_tasks_verbose
from paasta_tools.monitoring.replication_utils import backend_is_up
from paasta_tools.monitoring.replication_utils import match_backends_and_tasks
from paasta_tools.smartstack_tools import DEFAULT_SYNAPSE_PORT
from paasta_tools.smartstack_tools import get_backends
from paasta_tools.utils import _log
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import datetime_from_utc_to_local
from paasta_tools.utils import format_table
from paasta_tools.utils import is_under_replicated
from paasta_tools.utils import NoDockerImageError
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import remove_ansi_escape_sequences

log = logging.getLogger('__main__')
logging.basicConfig()


def start_marathon_job(service, instance, app_id, normal_instance_count, client, cluster):
    name = PaastaColors.cyan(compose_job_id(service, instance))
    _log(
        service=service,
        line="EmergencyStart: scaling %s up to %d instances" % (name, normal_instance_count),
        component='deploy',
        level='event',
        cluster=cluster,
        instance=instance
    )
    client.scale_app(app_id, instances=normal_instance_count, force=True)


def stop_marathon_job(service, instance, app_id, client, cluster):
    name = PaastaColors.cyan(compose_job_id(service, instance))
    _log(
        service=service,
        line="EmergencyStop: Scaling %s down to 0 instances" % (name),
        component='deploy',
        level='event',
        cluster=cluster,
        instance=instance
    )
    client.scale_app(app_id, instances=0, force=True)  # TODO do we want to capture the return val of any client calls?


def restart_marathon_job(service, instance, app_id, normal_instance_count, client, cluster):
    stop_marathon_job(service, instance, app_id, client, cluster)
    start_marathon_job(service, instance, app_id, normal_instance_count, client, cluster)


def scale_marathon_job(service, instance, app_id, delta, client, cluster):
    name = PaastaColors.cyan(compose_job_id(service, instance))
    _log(
        service=service,
        line="EmergencyScale: Scaling %s %s by %d instances" % (name, 'down' if delta < 0 else 'up', abs(int(delta))),
        component='deploy',
        level='event',
        cluster=cluster,
        instance=instance
    )
    client.scale_app(app_id, delta=int(delta), force=True)


def get_bouncing_status(service, instance, client, job_config):
    apps = marathon_tools.get_matching_appids(service, instance, client)
    bounce_method = job_config.get_bounce_method()
    app_count = len(apps)
    if app_count == 0:
        return PaastaColors.red("Stopped")
    elif app_count == 1:
        return PaastaColors.green("Running")
    elif app_count > 1:
        return PaastaColors.yellow("Bouncing (%s)" % bounce_method)
    else:
        return PaastaColors.red("Unknown (count: %s)" % app_count)


def status_desired_state(service, instance, client, job_config):
    status = get_bouncing_status(service, instance, client, job_config)
    desired_state = job_config.get_desired_state_human()
    return "State:      %s - Desired state: %s" % (status, desired_state)


def status_marathon_job(service, instance, app_id, normal_instance_count, client):
    name = PaastaColors.cyan(compose_job_id(service, instance))
    if marathon_tools.is_app_id_running(app_id, client):
        app = client.get_app(app_id)
        running_instances = app.tasks_running
        if len(app.deployments) == 0:
            deploy_status = PaastaColors.bold("Running")
        else:
            deploy_status = PaastaColors.yellow("Deploying")
        if running_instances >= normal_instance_count:
            status = PaastaColors.green("Healthy")
            instance_count = PaastaColors.green("(%d/%d)" % (running_instances, normal_instance_count))
        elif running_instances == 0:
            status = PaastaColors.yellow("Critical")
            instance_count = PaastaColors.red("(%d/%d)" % (running_instances, normal_instance_count))
        else:
            status = PaastaColors.yellow("Warning")
            instance_count = PaastaColors.yellow("(%d/%d)" % (running_instances, normal_instance_count))
        return "Marathon:   %s - up with %s instances. Status: %s." % (status, instance_count, deploy_status)
    else:
        red_not = PaastaColors.red("NOT")
        status = PaastaColors.red("Critical")
        return "Marathon:   %s - %s (app %s) is %s running in Marathon." % (status, name, app_id, red_not)


def get_verbose_status_of_marathon_app(app):
    """Takes a given marathon app object and returns the verbose details
    about the tasks, times, hosts, etc"""
    output = []
    create_datetime = datetime_from_utc_to_local(isodate.parse_datetime(app.version))
    output.append("  Marathon app ID: %s" % PaastaColors.bold(app.id))
    output.append("    App created: %s (%s)" % (str(create_datetime), humanize.naturaltime(create_datetime)))
    output.append("    Tasks:")

    rows = [("Mesos Task ID", "Host deployed to", "Deployed at what localtime")]
    for task in app.tasks:
        local_deployed_datetime = datetime_from_utc_to_local(task.staged_at)
        if task.host is not None:
            hostname = "%s:%s" % (task.host.split(".")[0], task.ports[0])
        else:
            hostname = "Unknown"
        rows.append((
            get_short_task_id(task.id),
            hostname,
            '%s (%s)' % (
                local_deployed_datetime.strftime("%Y-%m-%dT%H:%M"),
                humanize.naturaltime(local_deployed_datetime),
            )
        ))
    output.append('\n'.join(["      %s" % line for line in format_table(rows)]))
    if len(app.tasks) == 0:
        output.append("      No tasks associated with this marathon app")
    return app.tasks, "\n".join(output)


def status_marathon_job_verbose(service, instance, client):
    """Returns detailed information about a marathon apps for a service
    and instance. Does not make assumptions about what the *exact*
    appid is, but instead does a fuzzy match on any marathon apps
    that match the given service.instance"""
    all_tasks = []
    all_output = []
    # For verbose mode, we want to see *any* matching app. As it may
    # not be the one that we think should be deployed. For example
    # during a bounce we want to see the old and new ones.
    for app_id in marathon_tools.get_matching_appids(service, instance, client):
        if marathon_tools.is_app_id_running(app_id, client):
            app = client.get_app(app_id)
            tasks, output = get_verbose_status_of_marathon_app(app)
            all_tasks.extend(tasks)
            all_output.append(output)
        else:
            all_output.append("Warning: App %s not running." % app_id)
    return all_tasks, "\n".join(all_output)


def haproxy_backend_report(normal_instance_count, up_backends):
    """Given that a service is in smartstack, this returns a human readable
    report of the up backends"""
    # TODO: Take into account a configurable threshold, PAASTA-1102
    crit_threshold = 50
    under_replicated, ratio = is_under_replicated(num_available=up_backends,
                                                  expected_count=normal_instance_count,
                                                  crit_threshold=crit_threshold)
    if under_replicated:
        status = PaastaColors.red("Critical")
        count = PaastaColors.red("(%d/%d, %d%%)" % (up_backends, normal_instance_count, ratio))
    else:
        status = PaastaColors.green("Healthy")
        count = PaastaColors.green("(%d/%d)" % (up_backends, normal_instance_count))
    up_string = PaastaColors.bold('UP')
    return "%s - in haproxy with %s total backends %s in this namespace." % (status, count, up_string)


def format_haproxy_backend_row(backend, is_correct_instance):
    """Pretty Prints the status of a given haproxy backend
    Takes the fields described in the CSV format of haproxy:
    http://www.haproxy.org/download/1.5/doc/configuration.txt
    And tries to make a good guess about how to represent them in text
    """
    backend_name = backend['svname']
    backend_hostname = backend_name.split("_")[-1]
    backend_port = backend_name.split("_")[0].split(":")[-1]
    pretty_backend_name = "%s:%s" % (backend_hostname, backend_port)
    if backend['status'] == "UP":
        status = PaastaColors.default(backend['status'])
    elif backend['status'] == 'DOWN' or backend['status'] == 'MAINT':
        status = PaastaColors.red(backend['status'])
    else:
        status = PaastaColors.yellow(backend['status'])
    lastcheck = "%s/%s in %sms" % (backend['check_status'], backend['check_code'], backend['check_duration'])
    lastchange = humanize.naturaltime(datetime.timedelta(seconds=int(backend['lastchg'])))

    row = (
        '      %s' % pretty_backend_name,
        lastcheck,
        lastchange,
        status,
    )

    if is_correct_instance:
        return row
    else:
        return tuple(PaastaColors.grey(remove_ansi_escape_sequences(col)) for col in row)


def status_smartstack_backends(service, instance, job_config, cluster, tasks, expected_count, soa_dir, verbose):
    """Returns detailed information about smartstack backends for a service
    and instance.
    return: A newline separated string of the smarststack backend status
    """
    output = []
    nerve_ns = marathon_tools.read_namespace_for_service_instance(service, instance, cluster)
    service_instance = compose_job_id(service, nerve_ns)

    if instance != nerve_ns:
        ns_string = PaastaColors.bold(nerve_ns)
        output.append("Smartstack: N/A - %s is announced in the %s namespace." % (instance, ns_string))
        # If verbose mode is specified, then continue to show backends anyway, otherwise stop early
        if not verbose:
            return "\n".join(output)

    service_namespace_config = marathon_tools.load_service_namespace_config(service, instance, soa_dir=soa_dir)
    discover_location_type = service_namespace_config.get_discover()
    monitoring_blacklist = job_config.get_monitoring_blacklist()
    unique_attributes = get_mesos_slaves_grouped_by_attribute(
        attribute=discover_location_type, blacklist=monitoring_blacklist)
    if len(unique_attributes) == 0:
        output.append("Smartstack: ERROR - %s is NOT in smartstack at all!" % service_instance)
    else:
        output.append("Smartstack:")
        if verbose:
            output.append("  Haproxy Service Name: %s" % service_instance)
            output.append("  Backends:")

        output.extend(pretty_print_smartstack_backends_for_locations(
            service_instance,
            tasks,
            unique_attributes,
            expected_count,
            verbose
        ))
    return "\n".join(output)


def pretty_print_smartstack_backends_for_locations(service_instance, tasks, locations, expected_count, verbose):
    """
    Pretty prints the status of smartstack backends of a specified service and instance in the specified locations
    """
    rows = [("      Name", "LastCheck", "LastChange", "Status")]
    expected_count_per_location = int(expected_count / len(locations))
    for location in sorted(locations):
        hosts = locations[location]
        # arbitrarily choose the first host with a given attribute to query for replication stats
        synapse_host = hosts[0]
        sorted_backends = sorted(get_backends(service_instance,
                                              synapse_host=synapse_host,
                                              synapse_port=DEFAULT_SYNAPSE_PORT),
                                 key=lambda backend: backend['status'],
                                 reverse=True)  # Specify reverse so that backends in 'UP' are placed above 'MAINT'
        matched_tasks = match_backends_and_tasks(sorted_backends, tasks)
        running_count = sum(1 for backend, task in matched_tasks if backend and backend_is_up(backend))
        rows.append("    %s - %s" % (location, haproxy_backend_report(expected_count_per_location, running_count)))

        # If verbose mode is specified, show status of individual backends
        if verbose:
            for backend, task in matched_tasks:
                if backend is not None:
                    rows.append(format_haproxy_backend_row(backend, task is not None))
    return format_table(rows)


def get_short_task_id(task_id):
    """Return just the Marathon-generated UUID of a Mesos task id."""
    return task_id.split(marathon_tools.MESOS_TASK_SPACER)[-1]


def status_mesos_tasks(service, instance, normal_instance_count):
    job_id = marathon_tools.format_job_id(service, instance)
    running_and_active_tasks = get_running_tasks_from_active_frameworks(job_id)
    count = len(running_and_active_tasks)
    if count >= normal_instance_count:
        status = PaastaColors.green("Healthy")
        count = PaastaColors.green("(%d/%d)" % (count, normal_instance_count))
    elif count == 0:
        status = PaastaColors.red("Critical")
        count = PaastaColors.red("(%d/%d)" % (count, normal_instance_count))
    else:
        status = PaastaColors.yellow("Warning")
        count = PaastaColors.yellow("(%d/%d)" % (count, normal_instance_count))
    running_string = PaastaColors.bold('TASK_RUNNING')
    return "Mesos:      %s - %s tasks in the %s state." % (status, count, running_string)


def perform_command(command, service, instance, cluster, verbose, soa_dir, app_id=None, delta=None):
    """Performs a start/stop/restart/status/scale on an instance
    :param command: String of start, stop, restart, status or scale
    :param service: service name
    :param instance: instance name, like "main" or "canary"
    :param cluster: cluster name
    :param verbose: bool if the output should be verbose or not
    :returns: A unix-style return code
    """
    marathon_config = marathon_tools.load_marathon_config()
    job_config = marathon_tools.load_marathon_service_config(service, instance, cluster, soa_dir=soa_dir)
    try:
        complete_config = marathon_tools.create_complete_config(service, instance, soa_dir=soa_dir)
        if not app_id:
            app_id = complete_config['id']
    except NoDockerImageError:
        job_id = compose_job_id(service, instance)
        print "Docker image for %s not in deployments.json. Exiting. Has Jenkins deployed it?" % job_id
        return 1

    normal_instance_count = job_config.get_instances()
    normal_smartstack_count = marathon_tools.get_expected_instance_count_for_namespace(service, instance)
    proxy_port = marathon_tools.get_proxy_port_for_instance(service, instance, soa_dir=soa_dir)

    client = marathon_tools.get_marathon_client(marathon_config.get_url(), marathon_config.get_username(),
                                                marathon_config.get_password())
    if command == 'start':
        start_marathon_job(service, instance, app_id, normal_instance_count, client, cluster)
    elif command == 'stop':
        stop_marathon_job(service, instance, app_id, client, cluster)
    elif command == 'restart':
        restart_marathon_job(service, instance, app_id, normal_instance_count, client, cluster)
    elif command == 'status':
        # Setting up transparent cache for http API calls
        requests_cache.install_cache('paasta_serviceinit', backend='memory')

        print status_desired_state(service, instance, client, job_config)
        print status_marathon_job(service, instance, app_id, normal_instance_count, client)
        tasks, out = status_marathon_job_verbose(service, instance, client)
        if verbose:
            print out
        print status_mesos_tasks(service, instance, normal_instance_count)
        if verbose:
            print status_mesos_tasks_verbose(app_id, get_short_task_id)
        if proxy_port is not None:
            print status_smartstack_backends(
                service=service,
                instance=instance,
                cluster=cluster,
                job_config=job_config,
                tasks=tasks,
                expected_count=normal_smartstack_count,
                soa_dir=soa_dir,
                verbose=verbose,
            )
    elif command == 'scale':
        scale_marathon_job(service, instance, app_id, delta, client, cluster)
    else:
        # The command parser shouldn't have let us get this far...
        raise NotImplementedError("Command %s is not implemented!" % command)
    return 0


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
