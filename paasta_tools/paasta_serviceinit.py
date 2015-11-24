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

"""Usage: ./paasta_servceinit.py [-v] <servicename> <stop|start|restart|status|scale>

Interacts with the framework APIs to start/stop/restart/get status/scale for an
instance. Assumes that the credentials are available, so must run as root.
"""
import argparse
import logging
import sys

import service_configuration_lib

from paasta_tools import chronos_serviceinit
from paasta_tools import marathon_serviceinit
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import validate_service_instance


log = logging.getLogger('__main__')
logging.basicConfig()


def parse_args():
    parser = argparse.ArgumentParser(
        description='Runs start/stop/restart/status/scale on a PaaSTA service in a given cluster.',
    )
    parser.add_argument('-v', '--verbose', action='store_true', dest="verbose", default=False,
                        help="Print out more output regarding the state of the service")
    parser.add_argument('-D', '--debug', action='store_true', dest="debug", default=False,
                        help="Output debug logs regarding files, connections, etc")
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    parser.add_argument('service_instance', help='Instance to operate on. Eg: example_service.main')
    parser.add_argument('-a', '--appid', dest="app_id",
                        help="app ID as returned by paasta status -v to operate on")
    parser.add_argument('--delta', dest="delta",
                        help="Number of instances you want to scale up (positive number) or down (negative number)")
    command_choices = ['start', 'stop', 'restart', 'status', 'scale']
    parser.add_argument('command', choices=command_choices, help='Command to run. Eg: status')
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    if args.debug:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.WARNING)

    command = args.command
    service_instance = args.service_instance
    service, instance, _, __ = decompose_job_id(service_instance)

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
            app_id=args.app_id,
            delta=args.delta,
        )
        sys.exit(return_code)
    elif instance_type == 'chronos':
        return_code = chronos_serviceinit.perform_command(
            command=command,
            service=service,
            instance=instance,
            cluster=cluster,
            verbose=args.verbose,
            soa_dir=args.soa_dir,
        )
        sys.exit(return_code)
    else:
        log.error("I calculated an instance_type of %s for %s which I don't know how to handle. Exiting."
                  % (instance_type, compose_job_id(service, instance)))
        sys.exit(1)


if __name__ == "__main__":
    main()


# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
