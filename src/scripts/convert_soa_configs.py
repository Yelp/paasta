#!/usr/bin/env python

import os
import argparse
import logging
import yaml


log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Converts Yelp SOA configs to have a smartstack.yaml")
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest='verbose', default=False)
    parser.add_argument('soa_dir', help='SOA config directory')
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    soa_dir = args.soa_dir
    logging.basicConfig()
    if args.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)

    rootdir = os.path.abspath(soa_dir)
    for srv_dir in os.listdir(rootdir):
        total_dir = os.path.join(rootdir, srv_dir)

        smartstack_fname = os.path.join(total_dir, 'smartstack.yaml')
        service_fname    = os.path.join(total_dir, 'service.yaml')

        log.info("Converting %s", srv_dir)

        if os.path.exists(smartstack_fname) \
        and os.path.getsize(smartstack_fname) > 0:
            log.warning("SOA directory %s already has a non-empty smartstack.yaml, skipping", srv_dir)
            continue
        if not os.path.exists(service_fname):
            log.warning("SOA directory %s has no service.yaml, skipping", srv_dir)
            continue
        srv_yaml = yaml.load(open(service_fname, 'r'))
        if 'smartstack' not in srv_yaml:
            log.warning("SOA directory %s has no smartstack entry, skipping", srv_dir)
            continue
        smartstack_yaml = {'main': srv_yaml['smartstack']}
        log.info('smartstack.yaml contents:')
        log.info('%s', smartstack_yaml)
        yaml.dump(smartstack_yaml, open(smartstack_fname, 'w'),
                  explicit_start=True, default_flow_style=False)


if __name__ == "__main__":
    main()