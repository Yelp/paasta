# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import logging
import os.path
import time
from collections import defaultdict

from inotify.adapters import Inotify
from inotify.constants import IN_MODIFY
from inotify.constants import IN_MOVED_TO

from paasta_tools.chronos_tools import chronos_services_running_here
from paasta_tools.chronos_tools import load_chronos_job_config
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.marathon_tools import marathon_services_running_here
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_system_paasta_config

log = logging.getLogger(__name__)

DEFAULT_UPDATE_SECS = 5
DEFAULT_SYNAPSE_SERVICE_DIR = b'/var/run/synapse/services'


def parse_args(argv):
    parser = argparse.ArgumentParser(description='Monitor synapse changes and update service firewall rules')
    parser.add_argument('--synapse-service-dir', dest="synapse_service_dir",
                        default=DEFAULT_SYNAPSE_SERVICE_DIR,
                        help="Path to synapse service dir (default %(default)s)")
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="soa_dir",
                        default=DEFAULT_SOA_DIR,
                        help="define a different soa config directory (default %(default)s)")
    parser.add_argument('-u', '--update-secs', dest="update_secs",
                        default=DEFAULT_UPDATE_SECS, type=int,
                        help="Poll for new containers every N secs (default %(default)s)")
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true')

    args = parser.parse_args(argv)
    return args


def setup_logging(verbose):
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(level=level)


def run(args):
    # Main loop waiting on inotify file events
    inotify = Inotify(block_duration_s=1)  # event_gen blocks for 1 second
    inotify.add_watch(args.synapse_service_dir.encode(), IN_MOVED_TO | IN_MODIFY)
    services_by_dependencies_time = 0

    for event in inotify.event_gen():  # blocks for only up to 1 second at a time
        if services_by_dependencies_time + args.update_secs < time.time():
            services_by_dependencies = smartstack_dependencies_of_running_firewalled_services(
                soa_dir=args.soa_dir)
            services_by_dependencies_time = time.time()

        if event is None:
            continue

        process_inotify_event(event, services_by_dependencies)


def process_inotify_event(event, services_by_dependencies):
    filename = event[3]
    service_instance, suffix = os.path.splitext(filename)
    if suffix != '.json':
        return

    services_to_update = services_by_dependencies.get(service_instance, ())
    for service_to_update in services_to_update:
        log.debug('Update ', service_to_update)
        pass  # TODO: iptables added and removed here! :o)


def services_running_here(soa_dir):
    # Generator helper that yields (service, instance, config) of both marathon and chronos tasks
    cluster = load_system_paasta_config().get_cluster()

    for service, instance, port in marathon_services_running_here():
        config = load_marathon_service_config(service, instance, cluster, load_deployments=False, soa_dir=soa_dir)
        yield service, instance, config

    for service, instance, port in chronos_services_running_here():
        config = load_chronos_job_config(service, instance, cluster, load_deployments=False, soa_dir=soa_dir)
        yield service, instance, config


def smartstack_dependencies_of_running_firewalled_services(soa_dir=DEFAULT_SOA_DIR):
    dependencies_to_services = defaultdict(set)
    for service, instance, config in services_running_here(soa_dir):
        outbound_firewall = config.get_outbound_firewall()
        if not outbound_firewall:
            continue

        dependencies = config.get_dependencies()

        smartstack_dependencies = [d['smartstack'] for d in dependencies if d.get('smartstack')]
        for smartstack_dependency in smartstack_dependencies:
            # TODO: filter down to only services that have no proxy_port
            dependencies_to_services[smartstack_dependency].add((service, instance))

    return dependencies_to_services


def main(argv=None):
    args = parse_args(argv)
    setup_logging(args.verbose)
    run(args)
