#!/usr/bin/env python

import argparse
import service_configuration_lib


def main():
    args = parse_args()
    print args  # FIXME placeholder so flake8 doesn't complain about args being assigned to but never used


def parse_args():
    parser = argparse.ArgumentParser(description='Creates chronos configurations from yelpsoa-configs')
    parser.add_argument('--chronos-dir', dest='chronos_dir', metavar='CHRONOS_DIR',
                        help='chronos configuration directory')
    parser.add_argument('--ecosystem', dest='ecosystem', metavar='ECOSYSTEM',
                        help='ecosystem to generate configuration for')
    parser.add_argument('-d', '--soa-dir', dest='soa_dir', metavar='SOA_DIR',
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    main()
