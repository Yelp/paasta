#!/usr/bin/env python
"""Usage: ./marathon_servceinit.py [-v] <servicename> <stop|start|restart|status>

Interacts with the Marathon API to start/stop/restart/status a service.
Assumes that the credentials are available, so must run as root.

"""
import argparse
import datetime
import logging
import sys

import humanize
from mesos.cli.exceptions import SlaveDoesNotExist

from paasta_tools import marathon_tools
from paasta_tools.mesos_tools import get_non_running_mesos_tasks_for_service
from paasta_tools.mesos_tools import get_running_mesos_tasks_for_service
from paasta_tools.monitoring.replication_utils import get_replication_for_services
from paasta_tools.smartstack_tools import get_backends
from paasta_tools.utils import _log
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import datetime_from_utc_to_local

log = logging.getLogger('__main__')
log.addHandler(logging.StreamHandler(sys.stdout))

SYNAPSE_HOST_PORT = "localhost:3212"

RUNNING_TASK_FORMAT = '    {0[0]:<37}{0[1]:<20}{0[2]:<10}{0[3]:<6}{0[4]:}'
NON_RUNNING_TASK_FORMAT = '    {0[0]:<37}{0[1]:<20}{0[2]:<33}{0[3]:}'


def parse_args():
    parser = argparse.ArgumentParser(description='Runs start/stop/restart/status an existing Marathon service.')
    parser.add_argument('-v', '--verbose', action='store_true', dest="verbose", default=False,
                        help="Print out more output regarding the state of the service")
    parser.add_argument('-d', '--debug', action='store_true', dest="debug", default=False,
                        help="Output debug logs regarding files, connections, etc")
    parser.add_argument('service_instance', help='Instance to operate on. Eg: example_service.main')
    command_choices = ['start', 'stop', 'restart', 'status']
    parser.add_argument('command', choices=command_choices, help='Command to run. Eg: status')
    args = parser.parse_args()
    return args


def validate_service_instance(service, instance, cluster):
    log.info("Operating on cluster: %s" % cluster)
    all_services = marathon_tools.get_marathon_services_for_cluster(cluster)
    if (service, instance) not in all_services:
        print "Error: %s.%s doesn't look like it has been deployed to this cluster! (%s)" % (service, instance, cluster)
        log.info(all_services)
        sys.exit(3)
    return True


def start_marathon_job(service, instance, app_id, normal_instance_count, client, cluster):
    name = PaastaColors.cyan("%s.%s" % (service, instance))
    _log(
        service_name=service,
        line="EmergencyStart: scaling %s up to %d instances" % (name, normal_instance_count),
        component='deploy',
        level='event',
        cluster=cluster,
        instance=instance
    )
    client.scale_app(app_id, instances=normal_instance_count, force=True)


def stop_marathon_job(service, instance, app_id, client, cluster):
    name = PaastaColors.cyan("%s.%s" % (service, instance))
    _log(
        service_name=service,
        line="EmergencyStop: Scaling %s down to 0 instances" % (name),
        component='deploy',
        level='event',
        cluster=cluster,
        instance=instance
    )
    client.scale_app(app_id, instances=0, force=True)


def restart_marathon_job(service, instance, app_id, normal_instance_count, client, cluster):
    stop_marathon_job(service, instance, app_id, client, cluster)
    start_marathon_job(service, instance, app_id, normal_instance_count, client, cluster)


def get_bouncing_status(service, instance, client, complete_job_config):
    apps = marathon_tools.get_matching_appids(service, instance, client)
    bounce_method = complete_job_config.get_bounce_method()
    app_count = len(apps)
    if app_count == 0:
        return PaastaColors.red("Stopped")
    elif app_count == 1:
        return PaastaColors.green("Running")
    elif app_count > 1:
        return PaastaColors.yellow("Bouncing (%s)" % bounce_method)
    else:
        return PaastaColors.red("Unknown (count: %s)" % app_count)


def get_desired_state_human(complete_job_config):
    desired_state = complete_job_config.get_desired_state()
    if desired_state == 'start':
        return PaastaColors.bold('Started')
    elif desired_state == 'stop':
        return PaastaColors.red('Stopped')
    else:
        return PaastaColors.red('Unknown (desired_state: %s)' % desired_state)


def status_desired_state(service, instance, client, complete_job_config):
    status = get_bouncing_status(service, instance, client, complete_job_config)
    desired_state = get_desired_state_human(complete_job_config)
    return "State:      %s - Desired state: %s" % (status, desired_state)


def status_marathon_job(service, instance, app_id, normal_instance_count, client):
    name = PaastaColors.cyan("%s.%s" % (service, instance))
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
    create_datetime = datetime_from_utc_to_local(datetime.datetime.strptime(app.version, "%Y-%m-%dT%H:%M:%S.%fZ"))
    output.append("  Marathon app ID: %s" % PaastaColors.bold(app.id))
    output.append("    App created: %s (%s)" % (str(create_datetime), humanize.naturaltime(create_datetime)))
    output.append("    Tasks:  Mesos Task ID                  Host deployed to    Deployed at what localtime")
    for task in app.tasks:
        local_deployed_datetime = datetime_from_utc_to_local(task.staged_at)
        if task.host is not None:
            hostname = task.host.split(".")[0]
        else:
            hostname = "Unknown"
        format_tuple = (
            get_task_uuid(task.id),
            hostname,
            local_deployed_datetime.strftime("%Y-%m-%dT%H:%M"),
            humanize.naturaltime(local_deployed_datetime),
        )
        output.append('      {0[0]:<37}{0[1]:<20}{0[2]:<17}({0[3]:})'.format(format_tuple))
    if len(app.tasks) == 0:
        output.append("      No tasks associated with this marathon app")
    return "\n".join(output)


def status_marathon_job_verbose(service, instance, client):
    """Returns detailed information about a marathon apps for a service
    and instance. Does not make assumptions about what the *exact*
    appid is, but instead does a fuzzy match on any marathon apps
    that match the given service.instance"""
    output = []
    # For verbose mode, we want to see *any* matching app. As it may
    # not be the one that we think should be deployed. For example
    # during a bounce we want to see the old and new ones.
    for appid in marathon_tools.get_matching_appids(service, instance, client):
        app = client.get_app(appid)
        output.append(get_verbose_status_of_marathon_app(app))
    return "\n".join(output)


def haproxy_backend_report(normal_instance_count, up_backends):
    """ Given that a service is in smartstack, this returns a human readable
    report of the up backends """
    if up_backends >= normal_instance_count:
        status = PaastaColors.green("Healthy")
        count = PaastaColors.green("(%d/%d)" % (up_backends, normal_instance_count))
    elif up_backends == 0:
        status = PaastaColors.red("Critical")
        count = PaastaColors.red("(%d/%d)" % (up_backends, normal_instance_count))
    else:
        status = PaastaColors.yellow("Warning")
        count = PaastaColors.yellow("(%d/%d)" % (up_backends, normal_instance_count))
    up_string = PaastaColors.bold('UP')
    return "%s - in haproxy with %s total backends %s in this namespace." % (status, count, up_string)


def status_smartstack_backends(service, instance, normal_instance_count, cluster):
    nerve_ns = marathon_tools.read_namespace_for_service_instance(service, instance, cluster)
    if instance != nerve_ns:
        ns_string = PaastaColors.bold(nerve_ns)
        return "Smartstack: N/A - %s is announced in the %s namespace." % (instance, ns_string)
    else:
        try:
            service_instance = "%s.%s" % (service, instance)
            up_backends = get_replication_for_services(SYNAPSE_HOST_PORT, [service_instance])
            up_backend_count = up_backends[service_instance]
            report = haproxy_backend_report(normal_instance_count, up_backend_count)
            return "Smartstack: %s" % report
        except KeyError:
            return "Smartstack: ERROR - %s is NOT in smartstack at all!" % service_instance


def pretty_print_haproxy_backend(backend):
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

    format_tuple = (
        pretty_backend_name,
        lastcheck,
        lastchange,
        status,
    )
    return '    {0[0]:<32}{0[1]:<20}{0[2]:<16}{0[3]:}'.format(format_tuple)


def status_smartstack_backends_verbose(service, instance, cluster):
    """Returns detailed information about smartstack backends for a
    service and instance"""
    nerve_ns = marathon_tools.read_namespace_for_service_instance(service, instance, cluster)
    output = []
    # Only bother doing things if we are on the same namespace
    if instance == nerve_ns:
        service_instance = "%s.%s" % (service, instance)
        output.append("  Haproxy Service Name: %s" % service_instance)
        output.append("  Backends: Name                    LastCheck           LastChange      Status")
        sorted_backends = sorted(get_backends(service_instance),
                                 key=lambda backend: backend['status'],
                                 reverse=True)
        for backend in sorted_backends:
            output.append(pretty_print_haproxy_backend(backend))
        return "\n".join(output)
    else:
        return ""


def status_mesos_tasks(service, instance, normal_instance_count):
    running_tasks = get_running_mesos_tasks_for_service(service, instance)
    count = len(running_tasks)
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


def get_cpu_usage(task):
    """Calculates a metric of used_cpu/allocated_cpu
    To do this, we take the total number of cpu-seconds the task has consumed,
    (the sum of system and user time), OVER the total cpu time the task
    has been allocated.

    The total time a task has been allocated is the total time the task has
    been running (https://github.com/mesosphere/mesos/blob/0b092b1b0/src/webui/master/static/js/controllers.js#L140)
    multiplied by the "shares" a task has.
    """
    try:
        start_time = round(task['statuses'][0]['timestamp'])
        current_time = int(datetime.datetime.now().strftime('%s'))
        duration_seconds = current_time - start_time
        # The CPU shares has an additional .1 allocated to it for executor overhead.
        # We subtract this to the true number
        # (https://github.com/apache/mesos/blob/dc7c4b6d0bcf778cc0cad57bb108564be734143a/src/slave/constants.hpp#L100)
        cpu_shares = task.cpu_limit - .1
        allocated_seconds = duration_seconds * cpu_shares
        used_seconds = task.stats.get('cpus_system_time_secs', 0.0) + task.stats.get('cpus_user_time_secs', 0.0)
        if allocated_seconds == 0:
            return "Undef"
        percent = round(100 * (used_seconds / allocated_seconds), 1)
        percent_string = "%s%%" % percent
        if percent > 90:
            return PaastaColors.red(percent_string)
        else:
            return percent_string
    except (AttributeError, SlaveDoesNotExist):
        return "None"


def get_mem_usage(task):
    try:
        if task.mem_limit == 0:
            return "Undef"
        mem_percent = task.rss / task.mem_limit * 100
        mem_string = "%d/%dMB" % ((task.rss / 1024 / 1024), (task.mem_limit / 1024 / 1024))
        if mem_percent > 90:
            return PaastaColors.red(mem_string)
        else:
            return mem_string
    except (AttributeError, SlaveDoesNotExist):
        return "None"


def get_task_uuid(taskid):
    """Return just the UUID part of a mesos task id"""
    return taskid.split(".")[-1]


def get_short_hostname_from_task(task):
    try:
        slave_hostname = task.slave['hostname']
        return slave_hostname.split(".")[0]
    except (AttributeError, SlaveDoesNotExist):
        return 'Unknown'


def get_first_status_timestamp(task):
    """Gets the first status timestamp from a task id and returns a human
    readable string with the local time and a humanized duration:
    ``2015-01-30 08:45:19.108820 (an hour ago)``
    """
    try:
        start_time_string = task['statuses'][0]['timestamp']
        start_time = datetime.datetime.fromtimestamp(float(start_time_string))
        return "%s (%s)" % (start_time.strftime("%Y-%m-%dT%H:%M"), humanize.naturaltime(start_time))
    except (IndexError, SlaveDoesNotExist):
        return "Unknown"


def pretty_format_running_mesos_task(task):
    """Returns a pretty formatted string of a running mesos task attributes"""
    format_tuple = (
        get_task_uuid(task['id']),
        get_short_hostname_from_task(task),
        get_mem_usage(task),
        get_cpu_usage(task),
        get_first_status_timestamp(task),
    )
    return RUNNING_TASK_FORMAT.format(format_tuple)


def pretty_format_non_running_mesos_task(task):
    """Returns a pretty formatted string of a running mesos task attributes"""
    format_tuple = (
        get_task_uuid(task['id']),
        get_short_hostname_from_task(task),
        get_first_status_timestamp(task),
        task['state'],
    )
    return PaastaColors.grey(NON_RUNNING_TASK_FORMAT.format(format_tuple))


def status_mesos_tasks_verbose(service, instance):
    """Returns detailed information about the mesos tasks for a service"""
    output = []
    running_tasks = get_running_mesos_tasks_for_service(service, instance)
    output.append(RUNNING_TASK_FORMAT.format((
        "  Running Tasks:  Mesos Task ID",
        "Host deployed to",
        "Ram",
        "CPU",
        "Deployed at what localtime"
    )))
    for task in running_tasks:
        output.append(pretty_format_running_mesos_task(task))
    non_running_tasks = list(reversed(get_non_running_mesos_tasks_for_service(service, instance)[-10:]))
    output.append(PaastaColors.grey(NON_RUNNING_TASK_FORMAT.format((
        "  Non-Running Tasks:  Mesos Task ID",
        "Host deployed to",
        "Deployed at what localtime",
        "Status"
    ))))
    for task in non_running_tasks:
        output.append(pretty_format_non_running_mesos_task(task))
    return "\n".join(output)


def main():
    args = parse_args()
    if args.debug:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)

    command = args.command
    service_instance = args.service_instance
    service = service_instance.split(marathon_tools.ID_SPACER)[0]
    instance = service_instance.split(marathon_tools.ID_SPACER)[1]

    marathon_config = marathon_tools.load_marathon_config()
    cluster = marathon_config.get_cluster()
    validate_service_instance(service, instance, cluster)

    complete_job_config = marathon_tools.load_marathon_service_config(service, instance, cluster)
    app_id = marathon_tools.get_app_id(service, instance, marathon_config)
    normal_instance_count = complete_job_config.get_instances()
    normal_smartstack_count = marathon_tools.get_expected_instance_count_for_namespace(service, instance)

    client = marathon_tools.get_marathon_client(marathon_config['url'], marathon_config['user'],
                                                marathon_config['pass'])
    if command == 'start':
        start_marathon_job(service, instance, app_id, normal_instance_count, client, cluster)
    elif command == 'stop':
        stop_marathon_job(service, instance, app_id, client, cluster)
    elif command == 'restart':
        restart_marathon_job(service, instance, app_id, normal_instance_count, client, cluster)
    elif command == 'status':
        print status_desired_state(service, instance, client, complete_job_config)
        print status_marathon_job(service, instance, app_id, normal_instance_count, client)
        if args.verbose:
            print status_marathon_job_verbose(service, instance, client)
        print status_mesos_tasks(service, instance, normal_instance_count)
        if args.verbose:
            print status_mesos_tasks_verbose(service, instance)
        print status_smartstack_backends(service, instance, normal_smartstack_count, cluster)
        if args.verbose:
            print status_smartstack_backends_verbose(service, instance, cluster)
    else:
        # The command parser shouldn't have let us get this far...
        raise NotImplementedError("Command %s is not implemented!" % command)
    sys.exit(0)


if __name__ == "__main__":
    main()


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
