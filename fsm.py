#!/usr/bin/python

import optparse
from os.path import exists
import sys

from service_wizard.questions import _yamlize
from service_wizard.questions import get_smartstack_stanza
from service_wizard.questions import get_srvname
from service_wizard.service import Service


def parse_args():
    parser = optparse.OptionParser()
    parser.add_option("-y", "--yelpsoa-config-root", dest="yelpsoa_config_root", default=None, help="Path to root of yelpsoa-configs checkout")
    parser.add_option("-s", "--service-name", dest="srvname", default=None, help="Name of service being configured (--auto not available)")
    parser.add_option("-a", "--auto", dest="auto", default=False, action="store_true", help="Automatically calculate and use sane defaults. Exit violently if any values cannot be automatically calculated.")
    parser.add_option("-p", "--port", dest="port", default=None, help="Smartstack proxy port used by service.")

    opts, args = parser.parse_args()
    validate_options(parser, opts)
    return opts, args


def validate_options(parser, opts):
    """Does sys.exit() if an invalid combination of options is specified.
    Otherwise returns None (implicitly)."""
    if not opts.yelpsoa_config_root:
        parser.print_usage()
        sys.exit("I'd Really Rather You Didn't Fail To Provide --yelpsoa-config-root")
    if not exists(opts.yelpsoa_config_root):
        parser.print_usage()
        sys.exit(
            "I'd Really Rather You Didn't Use A Non-Existent --yelpsoa-config-root"
            "Like %s" % opts.yelpsoa_config_root
        )


def get_paasta_config(yelpsoa_config_root, srvname, auto, port):
    srvname = get_srvname(srvname, auto)
    smartstack_stanza = get_smartstack_stanza(yelpsoa_config_root, auto, port)
    return (srvname, smartstack_stanza)


def write_paasta_config(srv, smartstack_stanza):
    srv.io.write_file('smartstack.yaml', _yamlize(smartstack_stanza))


def main(opts, args):
    (srvname, smartstack_stanza) = get_paasta_config(opts.yelpsoa_config_root, opts.srvname, opts.auto, opts.port)
    srv = Service(srvname, opts.yelpsoa_config_root)
    write_paasta_config(srv, smartstack_stanza)


if __name__ == '__main__':
    opts, args = parse_args()
    main(opts, args)
