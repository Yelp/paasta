#!/usr/bin/env python
"""Usage: ./marathon_servceinit.py [-v] <servicename> <stop|start|restart|status>

Interacts with the Marathon API to start/stop/restart/status a service.
Assumes that the credentials are available, so must run as root.

"""
import argparse
import datetime
import logging
import time
import sys

import humanize

from paasta_tools import marathon_tools
from paasta_tools.paasta_cli.utils import PaastaColors
from paasta_tools.monitoring.replication_utils import get_replication_for_services
from paasta_tools.mesos_tools import get_running_mesos_tasks_for_service

log = logging.getLogger('__main__')
log.addHandler(logging.StreamHandler(sys.stdout))

SYNAPSE_HOST_PORT = "localhost:3212"


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


def datetime_from_utc_to_local(utc_datetime):
    now_timestamp = time.time()
    offset = datetime.datetime.fromtimestamp(now_timestamp) - datetime.datetime.utcfromtimestamp(now_timestamp)
    return utc_datetime + offset


def validate_service_instance(service, instance, cluster):
    log.info("Operating on cluster: %s" % cluster)
    all_services = marathon_tools.get_marathon_services_for_cluster(cluster)
    if (service, instance) not in all_services:
        log.info(all_services)
        print "Error: %s.%s doesn't look like it has been deployed to this cluster! (%s)" % (service, instance, cluster)
        sys.exit(3)
    return True


def start_marathon_job(service, instance, app_id, normal_instance_count, client):
    name = PaastaColors.cyan("%s.%s" % (service, instance))
    print "Scaling %s up to %d instances" % (name, normal_instance_count)
    client.scale_app(app_id, instances=normal_instance_count, force=True)


def stop_marathon_job(service, instance, app_id, client):
    name = PaastaColors.cyan("%s.%s" % (service, instance))
    print "Scaling %s down to 0 instances" % (name)
    client.scale_app(app_id, instances=0, force=True)


def restart_marathon_job(service, instance, app_id, normal_instance_count, client):
    stop_marathon_job(service, instance, app_id, client)
    start_marathon_job(service, instance, app_id, normal_instance_count, client)


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
        return "Marathon:   %s: - %s (app %s) is %s running in Marathon." % (status, name, app_id, red_not)


def get_verbose_status_of_marathon_app(app):
    """Takes a given marathon app object and returns the verbose details
    about the tasks, times, hosts, etc"""
    output = ""
    create_datetime = datetime.datetime.strptime(app.version, "%Y-%m-%dT%H:%M:%S.%fZ")
    output += "  Marathon app ID: %s\n" % PaastaColors.bold(app.id)
    output += "  App created: %s (%s)\n" % (str(create_datetime), humanize.naturaltime(create_datetime))
    output += "  Tasks:  Mesos Task ID                                                                       Host deployed to                        Deployed at what localtime\n"
    for task in app.tasks:
        local_deployed_datetime = datetime_from_utc_to_local(task.staged_at)
        format_tuple = (
            task.id,
            task.host,
            str(local_deployed_datetime),
            humanize.naturaltime(local_deployed_datetime),
        )
        output += '    {0[0]:<90}{0[1]:<40}{0[2]:<27}({0[3]:})\n'.format(format_tuple)
    return output


def status_marathon_job_verbose(service, instance, client):
    """Returns detailed information about a marathon apps for a service
    and instance. Does not make assumptions about what the *exact*
    appid is, but instead does a fuzzy match on any marathon apps
    that match the given service.instance"""
    output = ""
    # For verbose mode, we want to see *any* matching app. As it may
    # not be the one that we think should be deployed. For example
    # During a bounce we want to see the old and new ones.
    for appid in marathon_tools.get_matching_appids(service, instance, client):
        app = client.get_app(appid)
        output += get_verbose_status_of_marathon_app(app)
    return output


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


def status_smartstack_backends_verbose(service, instance, cluster):
    """Returns detailed information about smartstack backends for a service instance"""
    return None


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


def status_mesos_tasks_verbose(service, instance):
    """Returns detailed information about the mesos tasks for a service"""
    return None


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

    marathon_config = marathon_tools.get_config()
    cluster = marathon_tools.get_cluster()
    validate_service_instance(service, instance, cluster)

    complete_job_config = marathon_tools.read_service_config(service, instance, cluster)
    app_id = marathon_tools.get_app_id(service, instance, marathon_config)
    normal_instance_count = marathon_tools.get_instances(complete_job_config)
    normal_smartstack_count = marathon_tools.get_expected_instance_count_for_namespace(service, instance)

    client = marathon_tools.get_marathon_client(marathon_config['url'], marathon_config['user'],
                                                marathon_config['pass'])
    if command == 'start':
        start_marathon_job(service, instance, app_id, normal_instance_count, client)
    elif command == 'stop':
        stop_marathon_job(service, instance, app_id, client)
    elif command == 'restart':
        restart_marathon_job(service, instance, app_id, normal_instance_count, client)
    elif command == 'status':
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
