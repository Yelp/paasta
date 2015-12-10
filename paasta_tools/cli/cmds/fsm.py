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

from os.path import exists
from os.path import join
import sys

from service_configuration_lib import DEFAULT_SOA_DIR

from paasta_tools.cli.fsm.questions import _yamlize
from paasta_tools.cli.fsm.questions import get_clusternames_from_deploy_stanza
from paasta_tools.cli.fsm.questions import get_deploy_stanza
from paasta_tools.cli.fsm.questions import get_marathon_stanza
from paasta_tools.cli.fsm.questions import get_monitoring_stanza
from paasta_tools.cli.fsm.questions import get_service_stanza
from paasta_tools.cli.fsm.questions import get_smartstack_stanza
from paasta_tools.cli.fsm.questions import get_srvname
from paasta_tools.cli.fsm.service import Service
from paasta_tools.cli.utils import list_teams
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.utils import PaastaColors


def add_subparser(subparsers):
    fsm_parser = subparsers.add_parser(
        "fsm",
        description="Configure A New PaaSTA Service -- http://y/paasta-deploy For Details",
        help="Start A New Service From Scratch",
    )
    fsm_parser.add_argument(
        "-y", "--yelpsoa-config-root",
        dest="yelpsoa_config_root",
        default=DEFAULT_SOA_DIR,
        required=True,
        help="Path to root of yelpsoa-configs checkout (required)")
    fsm_parser.add_argument(
        "-s", "--service-name",
        dest="srvname",
        default=None,
        help="Name of service being configured (--auto not available)")
    fsm_parser.add_argument(
        "--description",
        dest="description",
        default=None,
        help="One line description of the service. If AUTO will have placeholder text")
    fsm_parser.add_argument(
        "--external-link",
        dest="external_link",
        default=None,
        help="Link to a reference doc for the service. If AUTO will have placeholder text")
    fsm_parser.add_argument(
        "-a",
        "--auto",
        dest="auto",
        default=False,
        action="store_true",
        help="Automatically calculate and use sane defaults. Exit violently if "
             "any values cannot be automatically calculated.",
    )
    fsm_parser.add_argument(
        "-p", "--port",
        dest="port",
        default=None,
        help="Smartstack proxy port used by service.")
    fsm_parser.add_argument(
        "-t", "--team",
        dest="team",
        default=None,
        help="Team responsible for the service. Used by various notification "
             "systems. (--auto not available)",
    ).completer = lazy_choices_completer(list_teams)
    fsm_parser.set_defaults(command=paasta_fsm)


def validate_args(args):
    """Does sys.exit() if an invalid combination of args is specified.
    Otherwise returns None (implicitly)."""
    if not exists(args.yelpsoa_config_root):
        sys.exit(
            "I'd Really Rather You Didn't Use A Non-Existent --yelpsoa-config-root"
            "Like %s" % args.yelpsoa_config_root
        )
    if args.auto and not args.srvname:
        sys.exit(
            "I'd Really Rather You Didn't Use --auto Without --service-name"
        )


def get_paasta_config(yelpsoa_config_root, srvname, auto, port, team, description, external_link):
    srvname = get_srvname(srvname, auto)
    smartstack_stanza = get_smartstack_stanza(yelpsoa_config_root, auto, port)
    monitoring_stanza = get_monitoring_stanza(auto, team)
    deploy_stanza = get_deploy_stanza()
    marathon_stanza = get_marathon_stanza()
    service_stanza = get_service_stanza(description, external_link, auto)
    return (srvname, service_stanza, smartstack_stanza, monitoring_stanza, deploy_stanza, marathon_stanza, team)


def write_paasta_config(
    srv,
    service_stanza,
    smartstack_stanza,
    monitoring_stanza,
    deploy_stanza,
    marathon_stanza,
):
    srv.io.write_file("service.yaml", _yamlize(service_stanza))
    srv.io.write_file("smartstack.yaml", _yamlize(smartstack_stanza))
    srv.io.write_file("monitoring.yaml", _yamlize(monitoring_stanza))
    srv.io.write_file("deploy.yaml", _yamlize(deploy_stanza))
    srv.io.write_file("marathon-SHARED.yaml", _yamlize(marathon_stanza))

    for clustername in get_clusternames_from_deploy_stanza(deploy_stanza):
        srv.io.symlink_file_relative("marathon-SHARED.yaml", "marathon-%s.yaml" % clustername)


def paasta_fsm(args):
    validate_args(args)
    (srvname, service_stanza, smartstack_stanza, monitoring_stanza, deploy_stanza, marathon_stanza, team) = (
        get_paasta_config(
            args.yelpsoa_config_root,
            args.srvname,
            args.auto,
            args.port,
            args.team,
            args.description,
            args.external_link,
        )
    )
    srv = Service(srvname, args.yelpsoa_config_root)
    write_paasta_config(
        srv,
        service_stanza,
        smartstack_stanza,
        monitoring_stanza,
        deploy_stanza,
        marathon_stanza,
    )
    print PaastaColors.yellow("               _  _(o)_(o)_  _")
    print PaastaColors.red("             ._\`:_ F S M _:' \_,")
    print PaastaColors.green("                 / (`---'\ `-.")
    print PaastaColors.cyan("              ,-`  _)    (_,")
    print "With My Noodly Appendage I Have Written Configs For"
    print
    print PaastaColors.bold("    %s" % srvname)
    print
    print "Customize Them If It Makes You Happy -- http://y/paasta For Details"
    print "Remember To Add, Commit, And Push When You're Done:"
    print
    print "cd %s" % join(args.yelpsoa_config_root, srvname)
    print "# Review And/Or Customize Files"
    print "git add ."
    print "git commit -m'Initial Commit For %s'" % srvname
    print "git push origin HEAD  # Pushmaster Or Ops Deputy Privs Required"
    print
