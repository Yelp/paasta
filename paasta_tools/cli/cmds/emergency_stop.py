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
from __future__ import absolute_import
from __future__ import unicode_literals

from paasta_tools.cli.utils import execute_paasta_serviceinit_on_remote_master
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_instances
from paasta_tools.cli.utils import list_services
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import list_clusters
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import paasta_print


def add_subparser(subparsers):
    status_parser = subparsers.add_parser(
        'emergency-stop',
        help="Stop a PaaSTA service instance in an emergency",
        description=(
            "Chronos jobs: Stops and kills and inflight run.\n"
            "Marathon apps: Not implemented."
        ),
    )
    status_parser.add_argument(
        '-s', '--service',
        help="Service that you want to stop. Like 'example_service'.",
    ).completer = lazy_choices_completer(list_services)
    status_parser.add_argument(
        '-i', '--instance',
        help="Instance of the service that you want to stop. Like 'main' or 'canary'.",
        required=True,
    ).completer = lazy_choices_completer(list_instances)
    status_parser.add_argument(
        '-c', '--cluster',
        help="The PaaSTA cluster that has the service instance you want to stop. Like 'norcal-prod'.",
        required=True,
    ).completer = lazy_choices_completer(list_clusters)
    status_parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    status_parser.set_defaults(command=paasta_emergency_stop)


def paasta_emergency_stop(args):
    """Performs an emergency stop on a given service instance on a given cluster
    """
    system_paasta_config = load_system_paasta_config()
    service = figure_out_service_name(args, soa_dir=args.soa_dir)
    paasta_print("Performing an emergency stop on %s..." % compose_job_id(service, args.instance))
    return_code, output = execute_paasta_serviceinit_on_remote_master(
        subcommand='stop',
        cluster=args.cluster,
        service=service,
        instances=args.instance,
        system_paasta_config=system_paasta_config,
    )
    paasta_print("Output: %s" % output)
    paasta_print("%s" % "\n".join(paasta_emergency_stop.__doc__.splitlines()[-7:]))
    paasta_print("To start this service again asap, run:")
    paasta_print(
        "paasta emergency-start --service %s --instance %s --cluster %s" % (service, args.instance, args.cluster))

    return return_code
