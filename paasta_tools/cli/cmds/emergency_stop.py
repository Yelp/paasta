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
        'emergency-stop',
        help="Stop a PaaSTA service instance in an emergency",
        description=(
            "'emergency-stop' stops a Marathon service instance by scaling it down to 0. If the "
            "provided 'instance' name refers to a Chronos job, 'emergency-stop' will cancel the "
            "chronos job if it is currently running."
        ),
        epilog=(
            "Warning: 'emergency-stop' does not interact with load balancers, so any in-flight "
            "traffic will be dropped after stopping. Additionally the 'desired state' of a service "
            "is not changed after an 'emergency-stop', therefore alerts will fire for the service "
            "after an emergency stop.\n\n"
            "'emergency-stop' is not a permanant declaration of state. If the operator wishes to "
            "stop a service permanatly, they should run 'paasta stop', or configure the service to "
            "have '0' instances. Otherwise, subsequent changes or bounces to a service will start "
            "it right back up."
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
        '-a', '--appid',
        help="The complete marathon appid to stop. Like 'example-service.main.gitf0cfd3a0.config7a2a00b7",
        required=False,
    )
    status_parser.add_argument(
        '-y', '--yelpsoa-config-root',
        default=DEFAULT_SOA_DIR,
        required=False,
        help="Path to root of yelpsoa-configs checkout",
    )
    status_parser.set_defaults(command=paasta_emergency_stop)


def paasta_emergency_stop(args):
    """Performs an emergency stop on a given service instance on a given cluster

    Warning: This command does not permanently stop the service. The next time the service is updated
    (config change, deploy, bounce, etc.), those settings will override the emergency stop.

    If you want this stop to be permanant, adjust the relevant config file to reflect that.
    For example, this can be done for Marathon apps by setting 'instances: 0', or
    for Chronos jobs by setting 'disabled: True'. Alternatively, remove the config yaml entirely.
    """
    system_paasta_config = load_system_paasta_config()
    service = figure_out_service_name(args, soa_dir=args.yelpsoa_config_root)
    print "Performing an emergency stop on %s..." % compose_job_id(service, args.instance)
    output = execute_paasta_serviceinit_on_remote_master('stop', args.cluster, service, args.instance,
                                                         system_paasta_config, app_id=args.appid)
    print "Output: %s" % output
    print "%s" % "\n".join(paasta_emergency_stop.__doc__.splitlines()[-7:])
    print "To start this service again asap, run:"
    print "paasta emergency-start --service %s --instance %s --cluster %s" % (service, args.instance, args.cluster)
