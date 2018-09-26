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
import datetime
import socket
import sys

import choice

from paasta_tools import remote_git
from paasta_tools import utils
from paasta_tools.chronos_tools import ChronosJobConfig
from paasta_tools.cli.cmds.status import add_instance_filter_arguments
from paasta_tools.cli.cmds.status import apply_args_filters
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.generate_deployments_for_service import get_latest_deployment_tag
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import paasta_print
from paasta_tools.utils import PaastaColors


def add_subparser(subparsers):
    for command, lower, upper, cmd_func in [
        ('start', 'start or restart', 'Start or restart', paasta_start),
        ('restart', 'start or restart', 'Start or restart', paasta_start),
        ('stop', 'stop', 'Stop', paasta_stop),
    ]:
        status_parser = subparsers.add_parser(
            command,
            help="%ss a PaaSTA service in a graceful way." % upper,
            description=(
                "%ss a PaaSTA service in a graceful way. This uses the Git control plane." % upper
            ),
            epilog=(
                "This command uses Git, and assumes access and authorization to the Git repo "
                "for the service is available."
            ),
        )
        add_instance_filter_arguments(status_parser, verb=lower)
        status_parser.add_argument(
            '-d', '--soa-dir',
            dest="soa_dir",
            metavar="SOA_DIR",
            default=DEFAULT_SOA_DIR,
            help="define a different soa config directory",
        )
        status_parser.set_defaults(command=cmd_func)


def format_tag(branch, force_bounce, desired_state):
    return f'refs/tags/paasta-{branch}-{force_bounce}-{desired_state}'


def make_mutate_refs_func(service_config, force_bounce, desired_state):
    """Create a function that will inform send_pack that we want to create tags
    corresponding to the set of branches passed, with the given force_bounce
    and desired_state parameters. These tags will point at the current tip of
    the branch they associate with.

    dulwich's send_pack wants a function that takes a dictionary of ref name
    to sha and returns a modified version of that dictionary. send_pack will
    then diff what is returned versus what was passed in, and inform the remote
    git repo of our desires."""
    def mutate_refs(refs):
        deploy_group = service_config.get_deploy_group()
        (_, head_sha) = get_latest_deployment_tag(refs, deploy_group)
        refs[format_tag(service_config.get_branch(), force_bounce, desired_state)] = \
            head_sha
        return refs
    return mutate_refs


def log_event(service_config, desired_state):
    user = utils.get_username()
    host = socket.getfqdn()
    line = "Issued request to change state of {} (an instance of {}) to '{}' by {}@{}".format(
        service_config.get_instance(), service_config.get_service(),
        desired_state, user, host,
    )
    utils._log(
        service=service_config.get_service(),
        level='event',
        cluster=service_config.get_cluster(),
        instance=service_config.get_instance(),
        component='deploy',
        line=line,
    )


def issue_state_change_for_service(service_config, force_bounce, desired_state):
    ref_mutator = make_mutate_refs_func(
        service_config=service_config,
        force_bounce=force_bounce,
        desired_state=desired_state,
    )
    remote_git.create_remote_refs(utils.get_git_url(service_config.get_service()), ref_mutator)
    log_event(
        service_config=service_config,
        desired_state=desired_state,
    )


def print_marathon_message(desired_state):
    if desired_state == "start":
        paasta_print(
            "This service will soon be gracefully started/restarted, replacing old instances according "
            "to the bounce method chosen in soa-configs. ",
        )
    elif desired_state == "stop":
        paasta_print(
            "This service will be gracefully stopped soon. It will be started back up again on the next deploy.\n"
            "To stop this service permanently. Set this in the soa-configs definition:\n"
            "\n"
            "    instances: 0\n",
        )


def print_chronos_message(desired_state):
    if desired_state == "start":
        paasta_print(
            "'Start' will tell Chronos to start scheduling the job. "
            "If you need the job to start regardless of the schedule, use 'paasta emergency-start'.",
        )
    elif desired_state == "stop":
        paasta_print(
            "'Stop' for a Chronos job will cause the job to be disabled until the "
            "next deploy or a 'start' command is issued.",
        )


def paasta_start_or_stop(args, desired_state):
    """Requests a change of state to start or stop given branches of a service."""
    soa_dir = args.soa_dir

    pargs = apply_args_filters(args)
    if len(pargs) == 0:
        return 1

    affected_services = {s for service_list in pargs.values() for s in service_list.keys()}
    if len(affected_services) > 1:
        paasta_print(PaastaColors.red("Warning: trying to start/stop/restart multiple services:"))

        for cluster, services_instances in pargs.items():
            paasta_print("Cluster %s:" % cluster)
            for service, instances in services_instances.items():
                paasta_print("    Service %s:" % service)
                paasta_print("        Instances %s" % ",".join(instances.keys()))

        if sys.stdin.isatty():
            confirm = choice.Binary('Are you sure you want to continue?', False).ask()
        else:
            confirm = False
        if not confirm:
            paasta_print()
            paasta_print("exiting")
            return 1

    invalid_deploy_groups = []
    marathon_message_printed, chronos_message_printed = False, False
    for cluster, services_instances in pargs.items():
        for service, instances in services_instances.items():
            try:
                remote_refs = remote_git.list_remote_refs(utils.get_git_url(service, soa_dir))
            except remote_git.LSRemoteException as e:
                msg = (
                    "Error talking to the git server: %s\n"
                    "This PaaSTA command requires access to the git server to operate.\n"
                    "The git server may be down or not reachable from here.\n"
                    "Try again from somewhere where the git server can be reached, "
                    "like your developer environment."
                ) % str(e)
                paasta_print(msg)
                return 1

            for instance in instances.keys():
                service_config = get_instance_config(
                    service=service,
                    cluster=cluster,
                    instance=instance,
                    soa_dir=soa_dir,
                    load_deployments=False,
                )
                deploy_group = service_config.get_deploy_group()
                (deploy_tag, _) = get_latest_deployment_tag(remote_refs, deploy_group)

                if deploy_tag not in remote_refs:
                    invalid_deploy_groups.append(deploy_group)
                else:
                    force_bounce = utils.format_timestamp(datetime.datetime.utcnow())
                    if isinstance(service_config, MarathonServiceConfig) and not marathon_message_printed:
                        print_marathon_message(desired_state)
                        marathon_message_printed = True
                    elif isinstance(service_config, ChronosJobConfig) and not chronos_message_printed:
                        print_chronos_message(desired_state)
                        chronos_message_printed = True

                    issue_state_change_for_service(
                        service_config=service_config,
                        force_bounce=force_bounce,
                        desired_state=desired_state,
                    )

    return_val = 0
    if invalid_deploy_groups:
        paasta_print("No branches found for %s in %s." %
                     (", ".join(invalid_deploy_groups), remote_refs))
        paasta_print("Has %s been deployed there yet?" % service)
        return_val = 1

    return return_val


def paasta_start(args):
    return paasta_start_or_stop(args, 'start')


def paasta_stop(args):
    return paasta_start_or_stop(args, 'stop')
