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
from paasta_tools.cli.utils import execute_paasta_serviceinit_on_remote_master
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_instances
from paasta_tools.cli.utils import list_services
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import list_clusters
from paasta_tools.utils import load_system_paasta_config


def add_subparser(subparsers):
    status_parser = subparsers.add_parser(
        'emergency-restart',
        help="Restarts a PaaSTA service instance in an emergency",
        description=(
            "'paasta emergency-restart' is useful in situations where the operator "
            "needs to bypass the normal git-based control plan, and needs to interact "
            "with the underlying APIs directly. For example, in an emergency situation "
            "it may be necessary to restart a Marathon service without doing a 'full bounce'."
            "'emergency-restart' can do this, but at the cost of the safety of the normal "
            "bouncing procedures. In other words, and emergency-restart is fast, but not safe "
            "and will cause dropped traffic.\n\n"
        ),
    )
    status_parser.add_argument(
        '-s', '--service',
        help="Service that you want to restart. Like 'example_service'.",
    ).completer = lazy_choices_completer(list_services)
    status_parser.add_argument(
        '-i', '--instance',
        help="Instance of the service that you want to restart. Like 'main' or 'canary'.",
        required=True,
    ).completer = lazy_choices_completer(list_instances)
    status_parser.add_argument(
        '-c', '--cluster',
        help="The PaaSTA cluster that has the service you want to restart. Like 'norcal-prod'.",
        required=True,
    ).completer = lazy_choices_completer(list_clusters)
    status_parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    status_parser.set_defaults(command=paasta_emergency_restart)


def paasta_emergency_restart(args):
    """Performs an emergency restart on a given service instance on a given cluster

    Warning: This command is only intended to be used in an emergency.
    It should not be needed in normal circumstances.
    """
    service = figure_out_service_name(args, args.soa_dir)
    system_paasta_config = load_system_paasta_config()
    print "Performing an emergency restart on %s...\n" % compose_job_id(service, args.instance)
    execute_paasta_serviceinit_on_remote_master(
        subcommand='restart',
        cluster=args.cluster,
        service=args.service,
        instances=args.instance,
        system_paasta_config=system_paasta_config
    )
    print "%s" % "\n".join(paasta_emergency_restart.__doc__.splitlines()[-7:])
    print "Run this to see the status:"
    print "paasta status --service %s --clusters %s" % (service, args.cluster)
