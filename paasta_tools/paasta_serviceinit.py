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

import requests_cache

from paasta_tools import chronos_serviceinit
from paasta_tools import marathon_serviceinit
from paasta_tools.cli.cmds.status import get_actual_deployments
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import validate_service_instance


log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Runs start/stop/restart/status on a PaaSTA service in a given cluster.',
    )
    parser.add_argument('-v', '--verbose', action='count', dest="verbose", default=0,
                        help="Print out more output regarding the state of the service. "
                             "Multiple -v options increase verbosity. Maximum is 2.")
    parser.add_argument('-D', '--debug', action='store_true', dest="debug", default=False,
                        help="Output debug logs regarding files, connections, etc")
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=DEFAULT_SOA_DIR,
                        help="define a different soa config directory")

    # service_instance will be replaced with (-s, -i) when CLI changes are made.
    parser.add_argument('service_instance', nargs='?',
                        help='Instance to operate on. Eg: example_service.main')
    parser.add_argument('-s', '--service', dest="service",
                        help="The name of the service to inspect")
    parser.add_argument('-i', '--instances', dest="instances",
                        help="A comma-separated list of instances to view. Eg: canary,main")

    parser.add_argument('-a', '--appid', dest="app_id",
                        help="app ID as returned by paasta status -v to operate on")
    parser.add_argument('--delta', dest="delta",
                        help="Number of instances you want to scale up (positive number) or down (negative number)")
    command_choices = ['start', 'stop', 'restart', 'status']
    parser.add_argument('command', choices=command_choices, help='Command to run. Eg: status')
    args = parser.parse_args()
    return args


def get_deployment_version(actual_deployments, cluster, instance):
    key = '.'.join((cluster, instance))
    return actual_deployments[key][:8] if key in actual_deployments else None


def main():
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

    for instance in instances:
        # For an instance, there might be multiple versions running, e.g. in crossover bouncing.
        # In addition, mesos master does not have information of a chronos service's git hash.
        # The git sha in deployment.json is simply used here.
        version = get_deployment_version(actual_deployments, cluster, instance)
        print 'instance: %s' % PaastaColors.blue(instance)
        print 'Git sha:    %s (desired)' % version

        try:
            instance_type = validate_service_instance(service, instance, cluster, args.soa_dir)
            if instance_type == 'marathon':
                return_code = marathon_serviceinit.perform_command(
                    command=command,
                    service=service,
                    instance=instance,
                    cluster=cluster,
                    verbose=args.verbose,
                    soa_dir=args.soa_dir,
                    app_id=args.app_id,
                    delta=args.delta,
                )
            elif instance_type == 'chronos':
                return_code = chronos_serviceinit.perform_command(
                    command=command,
                    service=service,
                    instance=instance,
                    cluster=cluster,
                    verbose=args.verbose,
                    soa_dir=args.soa_dir,
                )
            else:
                log.error("I calculated an instance_type of %s for %s which I don't know how to handle."
                          % (instance_type, compose_job_id(service, instance)))
                return_code = 1
        except:
            log.error('Exception raised while looking at service %s instance %s:' % (service, instance))
            log.error(traceback.format_exc())
            return_code = 1

        return_codes.append(return_code)

    sys.exit(max(return_codes))


if __name__ == "__main__":
    main()


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
