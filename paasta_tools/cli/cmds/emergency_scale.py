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
        'emergency-scale',
        help="Scale a PaaSTA service instance in Marathon without bouncing it",
        description=(
            "'emergency-scale' is used to scale a PaaSTA service instance by scaling it up or down "
            "in Marathon by N instances, where N is provided by the --delta argument.\n\n"
            "This command works by using SSH to execute commands directly on the Marathon servers, "
            "where API access and credentials are guaranteed to be available."
        ),
        epilog=(
            "Warning: Using emergency-scale to scale *down* a service will not interact with "
            "load balancers, and therefore will drop traffic."
        ),
    )
    status_parser.add_argument(
        '-s', '--service',
        help="Service that you want to scale. Like 'example_service'.",
    ).completer = lazy_choices_completer(list_services)
    status_parser.add_argument(
        '-i', '--instance',
        help="Instance of the service that you want to scale. Like 'main' or 'canary'.",
        required=True,
    ).completer = lazy_choices_completer(list_instances)
    status_parser.add_argument(
        '-c', '--cluster',
        help="The PaaSTA cluster that has the service instance you want to scale. Like 'norcal-prod'.",
        required=True,
    ).completer = lazy_choices_completer(list_clusters)
    status_parser.add_argument(
        '-a', '--appid',
        help="The complete marathon appid to scale. Like 'example-service.main.gitf0cfd3a0.config7a2a00b7",
        required=False,
    )
    status_parser.add_argument(
        '-y', '--yelpsoa-config-root',
        default=DEFAULT_SOA_DIR,
        required=False,
        help="Path to root of yelpsoa-configs checkout",
    )
    status_parser.add_argument(
        '--delta',
        required=True,
        help="Number of instances you want to scale up (positive number) or down (negative number)",
    )
    status_parser.set_defaults(command=paasta_emergency_scale)


def paasta_emergency_scale(args):
    """Performs an emergency scale on a given service instance on a given cluster

    Warning: This command does not permanently scale the service. The next time the service is updated
    (config change, deploy, bounce, etc.), those settings will override the emergency scale.

    If you want this scale to be permanant, adjust the relevant config file to reflect that.
    For example, this can be done for Marathon apps by setting 'instances: n'
    """
    service = figure_out_service_name(args, soa_dir=args.yelpsoa_config_root)
    system_paasta_config = load_system_paasta_config()
    print "Performing an emergency scale on %s..." % compose_job_id(service, args.instance)
    output = execute_paasta_serviceinit_on_remote_master('scale', args.cluster, service, args.instance,
                                                         system_paasta_config, app_id=args.appid, delta=args.delta)
    print "Output: %s" % output
    print "%s" % "\n".join(paasta_emergency_scale.__doc__.splitlines()[-7:])
