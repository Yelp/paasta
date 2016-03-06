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
import datetime
import socket

from service_configuration_lib import DEFAULT_SOA_DIR

from paasta_tools import remote_git
from paasta_tools import utils
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_instances
from paasta_tools.cli.utils import list_services


def add_subparser(subparsers):
    for command, lower, upper, cmd_func in [
        ('start', 'start or restart', 'Start or restart', paasta_start),
        ('restart', 'start or restart', 'Start or restart', paasta_start),
        ('stop', 'stop', 'Stop', paasta_stop)
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
        status_parser.add_argument(
            '-s', '--service',
            help='Service that you want to %s. Like example_service.' % lower,
        ).completer = lazy_choices_completer(list_services)
        status_parser.add_argument(
            '-i', '--instance',
            help='Instance of the service that you want to %s. Like "main" or "canary".' % lower,
            required=True,
        ).completer = lazy_choices_completer(list_instances)
        status_parser.add_argument(
            '-c', '--cluster',
            help='The PaaSTA cluster that has the service you want to %s. Like norcal-prod' % lower,
            required=True,
        ).completer = lazy_choices_completer(utils.list_clusters)
        status_parser.add_argument(
            '-d', '--soa-dir',
            dest="soa_dir",
            metavar="SOA_DIR",
            default=DEFAULT_SOA_DIR,
            help="define a different soa config directory",
        )
        status_parser.set_defaults(command=cmd_func)


def format_tag(branch, force_bounce, desired_state):
    return 'refs/tags/paasta-%s-%s-%s' % (branch, force_bounce, desired_state)


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
        refs[format_tag(service_config.get_branch(), force_bounce, desired_state)] = \
            refs['refs/heads/paasta-%s' % service_config.get_deploy_group()]
        return refs
    return mutate_refs


def log_event(service_config, desired_state):
    user = utils.get_username()
    host = socket.getfqdn()
    line = "Issued request to change state of %s to '%s' by %s@%s" % (
        service_config.get_instance(), desired_state, user, host)
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


def paasta_start_or_stop(args, desired_state):
    """Requests a change of state to start or stop given branches of a service."""
    instance = args.instance
    cluster = args.cluster
    soa_dir = args.soa_dir
    service = figure_out_service_name(args=args, soa_dir=soa_dir)

    service_config = get_instance_config(
        service=service,
        cluster=cluster,
        instance=instance,
        soa_dir=soa_dir,
        load_deployments=False,
    )

    try:
        remote_refs = remote_git.list_remote_refs(utils.get_git_url(service))
    except remote_git.LSRemoteException as e:
        msg = (
            "Error talking to the git server: %s\n"
            "This PaaSTA command requires access to the git server to operate.\n"
            "The git server may be down or not reachable from here.\n"
            "Try again from somewhere where the git server can be reached, "
            "like your developer environment."
        ) % str(e)
        print msg
        return 1

    if 'refs/heads/paasta-%s' % service_config.get_deploy_group() not in remote_refs:
        print "No branches found for %s in %s." % \
            (service_config.get_deploy_group(), remote_refs)
        print "Has it been deployed there yet?"
        return 1

    force_bounce = utils.format_timestamp(datetime.datetime.utcnow())
    issue_state_change_for_service(
        service_config=service_config,
        force_bounce=force_bounce,
        desired_state=desired_state,
    )


def paasta_start(args):
    return paasta_start_or_stop(args, 'start')


def paasta_stop(args):
    return paasta_start_or_stop(args, 'stop')
