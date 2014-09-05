#!/usr/bin/python

import argparse
from os.path import exists
import sys

from service_wizard.questions import _yamlize
from service_wizard.questions import get_marathon_stanza
from service_wizard.questions import get_monitoring_stanza
from service_wizard.questions import get_smartstack_stanza
from service_wizard.questions import get_srvname
from service_wizard.service import Service


def parse_args():
    parser = argparse.ArgumentParser(description="Configure A New PaaSTA Service")
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


def get_paasta_config(yelpsoa_config_root, srvname, auto, port):
    srvname = get_srvname(srvname, auto)
    smartstack_stanza = get_smartstack_stanza(yelpsoa_config_root, auto, port)
    marathon_stanza = get_marathon_stanza()
    monitoring_stanza = get_monitoring_stanza()
    return (srvname, smartstack_stanza, marathon_stanza, monitoring_stanza)


def write_paasta_config(srv,
    smartstack_stanza,
    marathon_stanza,
    monitoring_stanza,
):
    srv.io.write_file("smartstack.yaml", _yamlize(smartstack_stanza))
    srv.io.write_file("marathon-devc.yaml", _yamlize(marathon_stanza))
    srv.io.write_file("monitoring.yaml", _yamlize(monitoring_stanza))


def main(args):
    (srvname, smartstack_stanza, marathon_stanza, monitoring_stanza) = (
        get_paasta_config(
            args.yelpsoa_config_root,
            args.srvname,
            args.auto,
            args.port,
    ))
    srv = Service(srvname, args.yelpsoa_config_root)
    write_paasta_config(
        srv,
        smartstack_stanza,
        marathon_stanza,
        monitoring_stanza,
    )


if __name__ == "__main__":
    args = parse_args()
    main(args)
