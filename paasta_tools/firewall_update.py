import argparse
import logging
import os.path
import time
from collections import defaultdict

from inotify.adapters import Inotify
from inotify.constants import IN_MODIFY
from inotify.constants import IN_MOVED_TO

from paasta_tools import firewall
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import TimeoutError

log = logging.getLogger(__name__)

DEFAULT_UPDATE_SECS = 5


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Monitor synapse changes and update service firewall rules"
    )
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="soa_dir",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory (default %(default)s)",
    )
    parser.add_argument(
        "--synapse-service-dir",
        dest="synapse_service_dir",
        default=firewall.DEFAULT_SYNAPSE_SERVICE_DIR,
        help="Path to synapse service dir (default %(default)s)",
    )
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true")

    subparsers = parser.add_subparsers(
        help="mode to run firewall update in", dest="mode"
    )
    subparsers.required = True

    daemon_parser = subparsers.add_parser(
        "daemon",
        description=(
            "Run a daemon which watches updates to synapse backends and updates iptables rules."
        ),
    )
    daemon_parser.add_argument(
        "-u",
        "--update-secs",
        dest="update_secs",
        default=DEFAULT_UPDATE_SECS,
        type=int,
        help="Poll for new containers every N secs (default %(default)s)",
    )

    subparsers.add_parser(
        "cron",
        description=(
            "Do a one-time update of iptables rules to match the current running services."
        ),
    )

    args = parser.parse_args(argv)
    return args


def setup_logging(verbose):
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(level=level)


def run_daemon(args):
    # Main loop waiting on inotify file events
    inotify = Inotify(block_duration_s=1)  # event_gen blocks for 1 second
    inotify.add_watch(args.synapse_service_dir.encode(), IN_MOVED_TO | IN_MODIFY)
    services_by_dependencies_time = 0

    for event in inotify.event_gen():  # blocks for only up to 1 second at a time
        if services_by_dependencies_time + args.update_secs < time.time():
            services_by_dependencies = (
                smartstack_dependencies_of_running_firewalled_services(
                    soa_dir=args.soa_dir
                )
            )
            services_by_dependencies_time = time.time()

        if event is None:
            continue

        process_inotify_event(
            event, services_by_dependencies, args.soa_dir, args.synapse_service_dir
        )


def run_cron(args):
    with firewall.firewall_flock():
        firewall.general_update(args.soa_dir, args.synapse_service_dir)


def process_inotify_event(
    event, services_by_dependencies, soa_dir, synapse_service_dir
):
    filename = event[3].decode()
    log.debug(f"process_inotify_event on {filename}")

    service_instance, suffix = os.path.splitext(filename)
    if suffix != ".json":
        return

    services_to_update = services_by_dependencies.get(service_instance, ())
    if not services_to_update:
        return

    # filter active_service_groups() down to just the names in services_to_update
    service_groups = {
        service_group: macs
        for service_group, macs in firewall.active_service_groups().items()
        if service_group in services_to_update
    }

    try:
        with firewall.firewall_flock():
            firewall.ensure_service_chains(service_groups, soa_dir, synapse_service_dir)

        for service_to_update in services_to_update:
            log.debug(f"Updated {service_to_update}")
    except TimeoutError as e:
        log.error(
            "Unable to update firewalls for {} because time-out obtaining flock: {}".format(
                service_groups.keys(), e
            )
        )


def smartstack_dependencies_of_running_firewalled_services(soa_dir=DEFAULT_SOA_DIR):
    dependencies_to_services = defaultdict(set)
    for service, instance, _, _ in firewall.services_running_here():
        config = get_instance_config(
            service=service,
            instance=instance,
            cluster=load_system_paasta_config().get_cluster(),
            load_deployments=False,
            soa_dir=soa_dir,
        )
        outbound_firewall = config.get_outbound_firewall()
        if not outbound_firewall:
            continue

        dependencies = config.get_dependencies() or ()

        smartstack_dependencies = [
            d["smartstack"] for d in dependencies if d.get("smartstack")
        ]
        for smartstack_dependency in smartstack_dependencies:
            # TODO: filter down to only services that have no proxy_port
            dependencies_to_services[smartstack_dependency].add(
                firewall.ServiceGroup(service, instance)
            )

    return dependencies_to_services


def main(argv=None):
    args = parse_args(argv)
    setup_logging(args.verbose)
    {"daemon": run_daemon, "cron": run_cron}[args.mode](args)
