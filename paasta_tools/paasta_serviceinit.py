#!/usr/bin/env python
"""Usage: ./paasta_servceinit.py [-v] <servicename> <stop|start|restart|status>

Interacts with the framework APIs to start/stop/restart/status a on instance.
Assumes that the credentials are available, so must run as root.
"""
import argparse
import logging
import sys

import service_configuration_lib

from paasta_tools import paasta_chronos_serviceinit
from paasta_tools import marathon_serviceinit
from paasta_tools import marathon_tools
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config


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


def validate_service_instance(service, instance, cluster, soa_dir):
    log.info("Operating on cluster: %s" % cluster)
    marathon_services = get_services_for_cluster(cluster=cluster, instance_type='marathon', soa_dir=soa_dir)
    chronos_services = get_services_for_cluster(cluster=cluster, instance_type='chronos', soa_dir=soa_dir)
    if (service, instance) in marathon_services:
        return 'marathon'
    elif (service, instance) in chronos_services:
        return 'chronos'
    else:
        print "Error: %s.%s doesn't look like it has been deployed to this cluster! (%s)" % (service, instance, cluster)
        log.debug("Discovered marathon services %s" % marathon_services)
        log.debug("Discovered chronos services %s" % marathon_services)
        sys.exit(3)


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
    instance_type = validate_service_instance(service, instance, cluster, args.soa_dir)
    if instance_type == 'marathon':
        return_code = marathon_serviceinit.perform_command(
            command=command,
            service=service,
            instance=instance,
            cluster=cluster,
            verbose=args.verbose,
            soa_dir=args.soa_dir,
        )
        sys.exit(return_code)
    elif instance_type == 'chronos':
        return_code = paasta_chronos_serviceinit.perform_command(
            command=command,
            service=service,
            instance=instance,
        )
        sys.exit(return_code)
    else:
        log.error(
            "I calculated an instance_type of %s for %s%s%s which I don't know how to handle. Exiting." %
            instance_type,
            service,
            marathon_tools.ID_SPACER,
            instance,
        )
        sys.exit(1)

if __name__ == "__main__":
    main()


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
