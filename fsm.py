#!/usr/bin/python

import argparse
from os.path import exists
import sys

from service_wizard.questions import _yamlize
from service_wizard.questions import get_clusternames_from_deploy_stanza
from service_wizard.questions import get_deploy_stanza
from service_wizard.questions import get_marathon_stanza
from service_wizard.questions import get_monitoring_stanza
from service_wizard.questions import get_smartstack_stanza
from service_wizard.questions import get_srvname
from service_wizard.service import Service


def parse_args():
    parser = argparse.ArgumentParser(description="Configure A New PaaSTA Service -- http://y/paasta For Details")
    parser.add_argument(
        "-y", "--yelpsoa-config-root",
        dest="yelpsoa_config_root",
        default=None,
        required=True,
        help="Path to root of yelpsoa-configs checkout (required)")
    parser.add_argument(
        "-s", "--service-name",
        dest="srvname",
        default=None,
        help="Name of service being configured (--auto not available)")
    parser.add_argument(
        "-a",
        "--auto",
        dest="auto",
        default=False,
        action="store_true",
        help="Automatically calculate and use sane defaults. Exit violently if "
            "any values cannot be automatically calculated.",
    )
    parser.add_argument(
        "-p", "--port",
        dest="port",
        default=None,
        help="Smartstack proxy port used by service.")
    parser.add_argument(
        "-t", "--team",
        dest="team",
        default=None,
        help="Team responsible for the service. Used by various notification "
            "systems. (--auto not available)",
    )

    args = parser.parse_args()
    validate_args(parser, args)
    return args


def validate_args(parser, args):
    """Does sys.exit() if an invalid combination of args is specified.
    Otherwise returns None (implicitly)."""
    if not exists(args.yelpsoa_config_root):
        parser.print_usage()
        sys.exit(
            "I'd Really Rather You Didn't Use A Non-Existent --yelpsoa-config-root"
            "Like %s" % args.yelpsoa_config_root
        )
    if args.auto and not args.srvname:
        parser.print_usage()
        sys.exit(
            "I'd Really Rather You Didn't Use --auto Without --service-name"
        )


def get_paasta_config(yelpsoa_config_root, srvname, auto, port, team):
    srvname = get_srvname(srvname, auto)
    smartstack_stanza = get_smartstack_stanza(yelpsoa_config_root, auto, port)
    monitoring_stanza = get_monitoring_stanza(auto, team)
    deploy_stanza = get_deploy_stanza()
    marathon_stanza = get_marathon_stanza()
    return (srvname, smartstack_stanza, monitoring_stanza, deploy_stanza, marathon_stanza, team)


def write_paasta_config(srv,
    smartstack_stanza,
    monitoring_stanza,
    deploy_stanza,
    marathon_stanza,
):
    srv.io.write_file("smartstack.yaml", _yamlize(smartstack_stanza))
    srv.io.write_file("monitoring.yaml", _yamlize(monitoring_stanza))
    srv.io.write_file("deploy.yaml", _yamlize(deploy_stanza))
    srv.io.write_file("marathon-SHARED.yaml", _yamlize(marathon_stanza))

    for clustername in get_clusternames_from_deploy_stanza(deploy_stanza):
        srv.io.symlink_file("marathon-SHARED.yaml", "marathon-%s.yaml" % clustername)


def main(args):
    (srvname, smartstack_stanza, monitoring_stanza, deploy_stanza, marathon_stanza, team) = (
        get_paasta_config(
            args.yelpsoa_config_root,
            args.srvname,
            args.auto,
            args.port,
            args.team,
    ))
    srv = Service(srvname, args.yelpsoa_config_root)
    write_paasta_config(
        srv,
        smartstack_stanza,
        monitoring_stanza,
        deploy_stanza,
        marathon_stanza,
    )
    print "With My Noodly Appendage I Have Written Configs For"
    print
    print "    %s" % srvname
    print
    print "Customize Them If It Makes You Happy -- http://y/paasta For Details"
    print "Remember To Add, Commit, And Push When You're Done!"


if __name__ == "__main__":
    args = parse_args()
    main(args)
