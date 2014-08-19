#!/usr/bin/python

import optparse
import sys

from service_wizard.questions import get_srvname
from service_wizard.service import Service


def parse_args():
    parser = optparse.OptionParser()
    parser.add_option("-y", "--yelpsoa-config-root", dest="yelpsoa_config_root", default=None, help="Path to root of yelpsoa-configs checkout")
    parser.add_option("-a", "--auto", dest="auto", default=False, action="store_true", help="Automatically calculate and use sane defaults. Exit violently if any values cannot be automatically calculated.")
    parser.add_option("-s", "--service-name", dest="srvname", default=None, help="Name of service being configured (--auto not available)")

    opts, args = parser.parse_args()
    validate_options(parser, opts)
    return opts, args


def validate_options(parser, opts):
    """Does sys.exit() if an invalid combination of options is specified.
    Otherwise returns None (implicitly)."""
    if not opts.yelpsoa_config_root:
        parser.print_usage()
        sys.exit("ERROR: --yelpsoa-config-root is required!")


def get_paasta_config(srvname, auto):
    srvname = get_srvname(srvname, auto)
    return srvname


def main(opts, args):
    srvname = get_paasta_config(opts.srvname, opts.auto)
    srv = Service(srvname, opts.yelpsoa_config_root)
    #do_paasta_steps(srv)
    print srv.__dict__


if __name__ == '__main__':
    opts, args = parse_args()
    main(opts, args)
