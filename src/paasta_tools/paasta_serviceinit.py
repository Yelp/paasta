#!/usr/bin/env python
"""Usage: ./marathon_servceinit.py [-v] <servicename> <stop|start|restart|status>

Interacts with the Marathon API to start/stop/restart/status a service.
Assumes that the credentials are available, so must run as root.

"""
import argparse
import sys
import logging

from paasta_tools import marathon_tools
from paasta_tools.paasta_cli.utils import PaastaColors

log = logging.getLogger('__main__')
log.addHandler(logging.StreamHandler(sys.stdout))


def parse_args():
    parser = argparse.ArgumentParser(description='Runs start/stop/restart/status an existing Marathon service.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest="verbose", default=False)
    parser.add_argument('service_instance', help='Instance to operate on. Eg: example_service.main')
    command_choices = ['start', 'stop', 'restart', 'status']
    parser.add_argument('command', choices=command_choices, help='Command to run. Eg: status')
    args = parser.parse_args()
    return args


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
            status = PaastaColors.bold("Running")
        else:
            status = PaastaColors.yellow("Deploying")
        if running_instances >= normal_instance_count:
            instance_count = PaastaColors.green("(%d/%d)" % (running_instances, normal_instance_count))
            print "%s exists in marathon with %s instances. Status: %s" % (name, instance_count, status)
            sys.exit(0)
        else:
            instance_count = PaastaColors.red("(%d/%d)" % (running_instances, normal_instance_count))
            print "%s exists in marathon with %s instances Status: %s." % (name, instance_count, status)
            sys.exit(1)
    else:
        red_not = PaastaColors.red("NOT")
        print "CRIT: %s (app %s) is %s running in Marathon." % (name, app_id, red_not)
        sys.exit(1)


def main():
    args = parse_args()
    if args.verbose:
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

    client = marathon_tools.get_marathon_client(marathon_config['url'], marathon_config['user'],
                                                marathon_config['pass'])
    if command == 'start':
        start_marathon_job(service, instance, app_id, normal_instance_count, client)
    elif command == 'stop':
        stop_marathon_job(service, instance, app_id, client)
    elif command == 'restart':
        restart_marathon_job(service, instance, app_id, normal_instance_count, client)
    elif command == 'status':
        status_marathon_job(service, instance, app_id, normal_instance_count, client)
    else:
        # The command parser shouldn't have let us get this far...
        raise NotImplementedError("Command %s is not implemented!" % command)
    sys.exit(0)


if __name__ == "__main__":
    main()

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
