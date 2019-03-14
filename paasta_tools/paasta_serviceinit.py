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
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional

import requests_cache
from chronos import ChronosClient

from paasta_tools import chronos_serviceinit
from paasta_tools import chronos_tools
from paasta_tools import marathon_serviceinit
from paasta_tools import marathon_tools
from paasta_tools import paasta_native_serviceinit
from paasta_tools import paasta_remote_run
from paasta_tools.cli.cmds.status import get_actual_deployments
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import validate_service_instance

log = logging.getLogger(__name__)
# kazoo can be really noisy - turn it down
logging.getLogger("kazoo").setLevel(logging.CRITICAL)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Runs start/stop/restart/status on a PaaSTA service in a given cluster.',
    )
    parser.add_argument(
        '-v', '--verbose', action='count', dest="verbose", default=0,
        help="Print out more output regarding the state of the service. "
             "Multiple -v options increase verbosity. Maximum is 2.",
    )
    parser.add_argument(
        '-D', '--debug', action='store_true', dest="debug", default=False,
        help="Output debug logs regarding files, connections, etc",
    )
    parser.add_argument(
        '-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )

    # service_instance will be replaced with (-s, -i) when CLI changes are made.
    parser.add_argument(
        'service_instance', nargs='?',
        help='Instance to operate on. Eg: example_service.main',
    )
    parser.add_argument(
        '-s', '--service', dest="service",
        help="The name of the service to inspect",
    )
    parser.add_argument(
        '-i', '--instances', dest="instances",
        help="A comma-separated list of instances to view. Eg: canary,main",
    )

    parser.add_argument(
        '-a', '--appid', dest="app_id",
        help="app ID as returned by paasta status -v to operate on",
    )
    parser.add_argument(
        '--delta', dest="delta",
        help="Number of instances you want to scale up (positive number) or down (negative number)",
    )
    command_choices = ['start', 'stop', 'restart', 'status']
    parser.add_argument('command', choices=command_choices, help='Command to run. Eg: status')
    args = parser.parse_args()
    return args


def get_deployment_version(
    actual_deployments: Mapping[str, str],
    cluster: str,
    instance: str,
) -> Optional[str]:
    key = '.'.join((cluster, instance))
    return actual_deployments[key][:8] if key in actual_deployments else None


class PaastaClients():
    _cached: bool
    _marathon: Optional[marathon_tools.MarathonClients]
    _chronos: Optional[ChronosClient]

    def __init__(self, cached: bool = False) -> None:
        self._cached = cached
        self._marathon = None
        self._chronos = None

    def marathon(self) -> marathon_tools.MarathonClients:
        if self._marathon is None:
            system_paasta_config = load_system_paasta_config()
            marathon_servers = marathon_tools.get_marathon_servers(system_paasta_config)
            self._marathon = marathon_tools.get_marathon_clients(marathon_servers, cached=True)
        return self._marathon

    def chronos(self) -> ChronosClient:
        if self._chronos is None:
            chronos_config = chronos_tools.load_chronos_config()
            self._chronos = chronos_tools.get_chronos_client(chronos_config, cached=self._cached)
        return self._chronos


def main() -> None:
    args = parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    instances = []
    return_codes = []
    command = args.command
    if (args.service_instance):
        service_instance = args.service_instance
        service, instance, _, __ = decompose_job_id(service_instance)
        instances.append(instance)
    elif (args.service and args.instances):
        service = args.service
        instances = args.instances.split(',')
    else:
        log.error("The name of service or the name of instance to inspect is missing. Exiting.")
        sys.exit(1)

    # Setting up transparent cache for http API calls
    requests_cache.install_cache("paasta_serviceinit", backend="memory")

    cluster = load_system_paasta_config().get_cluster()
    actual_deployments = get_actual_deployments(service, args.soa_dir)
    clients = PaastaClients(cached=(command == 'status'))

    instance_types = ['marathon', 'chronos', 'paasta_native', 'adhoc']
    instance_types_map: Dict[str, List[str]] = {it: [] for it in instance_types}
    for instance in instances:
        try:
            instance_type = validate_service_instance(
                service, instance, cluster, args.soa_dir,
            )
        except Exception:
            log.error(
                (
                    'Exception raised while looking at service %s instance %s:'
                ).format(service, instance),
            )
            log.error(traceback.format_exc())
            return_codes.append(1)
            continue

        if instance_type not in instance_types:
            log.error(
                (
                    "I calculated an instance_type of {} for {} which I don't "
                    "know how to handle."
                ).format(
                    instance_type, compose_job_id(service, instance),
                ),
            )
            return_codes.append(1)
        else:
            instance_types_map[instance_type].append(instance)

    remote_run_frameworks = None
    if len(instance_types_map['adhoc']) > 0:
        remote_run_frameworks = paasta_remote_run.remote_run_frameworks()

    service_config_loader = PaastaServiceConfigLoader(service)

    for instance_type in instance_types:

        if instance_type == 'marathon':
            job_configs = {jc.instance: jc for jc in service_config_loader.instance_configs(
                cluster=cluster,
                instance_type_class=marathon_tools.MarathonServiceConfig,
            )}

        for instance in instance_types_map[instance_type]:
            try:
                version = get_deployment_version(
                    actual_deployments, cluster, instance,
                )
                paasta_print('instance: %s' % PaastaColors.blue(instance))
                paasta_print('Git sha:    %s (desired)' % version)

                if instance_type == 'marathon':
                    return_code = marathon_serviceinit.perform_command(
                        command=command,
                        service=service,
                        instance=instance,
                        cluster=cluster,
                        verbose=args.verbose,
                        soa_dir=args.soa_dir,
                        app_id=args.app_id,
                        clients=clients.marathon(),
                        job_config=job_configs[instance],
                    )
                elif instance_type == 'chronos':
                    return_code = chronos_serviceinit.perform_command(
                        command=command,
                        service=service,
                        instance=instance,
                        cluster=cluster,
                        verbose=args.verbose,
                        soa_dir=args.soa_dir,
                        client=clients.chronos(),
                    )
                elif instance_type == 'paasta_native':
                    return_code = paasta_native_serviceinit.perform_command(
                        command=command,
                        service=service,
                        instance=instance,
                        cluster=cluster,
                        verbose=args.verbose,
                        soa_dir=args.soa_dir,
                    )
                elif instance_type == 'adhoc':
                    if command != 'status':
                        raise NotImplementedError
                    paasta_remote_run.remote_run_list_report(
                        service=service,
                        instance=instance,
                        cluster=cluster,
                        frameworks=remote_run_frameworks,
                    )
                    return_code = 0
            except Exception:
                log.error(
                    (
                        'Exception raised while looking at service {} '
                        'instance {}:'
                    ).format(service, instance),
                )
                log.error(traceback.format_exc())
                return_code = 1

            return_codes.append(return_code)

    sys.exit(max(return_codes))


if __name__ == "__main__":
    main()


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
