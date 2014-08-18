#!/usr/bin/python

import optparse
import sys

from service_wizard.questions import ask_srvname
from service_wizard.service import Service


def parse_args():
    parser = optparse.OptionParser()
    parser.add_option("-y", "--yelpsoa-config-root", dest="yelpsoa_config_root", default=None, help="Path to root of yelpsoa-configs checkout")
    parser.add_option("-s", "--service-name", dest="srvname", default=None, help="Name of service being configured")

    opts, args = parser.parse_args()
    validate_options(parser, opts)
    return opts, args

def validate_options(parser, opts):
    """Does sys.exit() if an invalid combination of options is specified.
    Otherwise returns None (implicitly)."""
    if not opts.yelpsoa_config_root:
        parser.print_usage()
        sys.exit("ERROR: --yelpsoa-config-root is required!")

def ask_paasta_questions(srvname):
    srvname = get_srvname(srvname)
    return srvname


def main(opts, args):

    srvname = ask_paasta_questions()
    srv = Service(srvname, opts.yelpsoa_config_root)
    #do_paasta_steps(srv)
    print srv


if __name__ == '__main__':
    opts, args = parse_args()
    main(opts, args)
