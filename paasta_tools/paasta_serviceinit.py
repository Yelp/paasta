#!/usr/bin/env python
"""Usage: ./paasta_servceinit.py [-v] <servicename> <stop|start|restart|status>

Interacts with the framework APIs to start/stop/restart/status a on instance.
Assumes that the credentials are available, so must run as root.
"""
import argparse
import logging
import sys

import service_configuration_lib

from paasta_tools import marathon_tools
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config
from paasta_tools import marathon_serviceinit

log = logging.getLogger('__main__')
log.addHandler(logging.StreamHandler(sys.stdout))


def parse_args():
    parser = argparse.ArgumentParser(description='Runs start/stop/restart/status an existing Marathon service.')
    parser.add_argument('-v', '--verbose', action='store_true', dest="verbose", default=False,
                        help="Print out more output regarding the state of the service")
    parser.add_argument('-D', '--debug', action='store_true', dest="debug", default=False,
                        help="Output debug logs regarding files, connections, etc")
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    parser.add_argument('service_instance', help='Instance to operate on. Eg: example_service.main')
    command_choices = ['start', 'stop', 'restart', 'status']
    parser.add_argument('command', choices=command_choices, help='Command to run. Eg: status')
    args = parser.parse_args()
    return args


def validate_service_instance(service, instance, cluster):
    log.info("Operating on cluster: %s" % cluster)
    all_services = get_services_for_cluster(cluster=cluster, instance_type='marathon')
    if (service, instance) not in all_services:
        print "Error: %s.%s doesn't look like it has been deployed to this cluster! (%s)" % (service, instance, cluster)
        log.info(all_services)
        sys.exit(3)
    return True


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

    cluster = load_system_paasta_config().get_cluster()
    marathon_serviceinit.validate_service_instance(service, instance, cluster)
    marathon_serviceinit.perform_command(command=command,
                                         service=service,
                                         instance=instance,
                                         cluster=cluster,
                                         verbose=args.verbose,
                                         soa_dir=args.soa_dir)


if __name__ == "__main__":
    main()


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
