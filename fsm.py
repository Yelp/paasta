#!/usr/bin/python

import optparse
import sys


def parse_args():
    parser = optparse.OptionParser()
    parser.add_option("-y", "--yelpsoa-config-root", dest="yelpsoa_config_root", default=None, help="Path to root of yelpsoa-configs checkout")

    opts, args = parser.parse_args()
    validate_options(parser, opts)
    return opts, args

def validate_options(parser, opts):
    """Does sys.exit() if an invalid combination of options is specified.
    Otherwise returns None (implicitly)."""
    if not opts.yelpsoa_config_root:
        parser.print_usage()
        sys.exit("ERROR: --yelpsoa-config-root is required!")
