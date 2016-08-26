#!/usr/bin/env python
# We're finding orphaned nerve service instance nodes in ZK.  These nodes
# contain no data and have no ephemeral owner.  The cause is currently unclear.
# This script will remove those nodes.  See SRV-1418 for more information.
import argparse
import glob
import logging
import os

import kazoo.client
import kazoo.exceptions
import yaml
# When we build .debs of paasta, we make sure to depend on libyaml, so that CLoader is available. However, libyaml is
# not always available when paasta is installed as a python package (such as when running the unit tests with tox).
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


# CEP 355 Zookeepers
ZK_DEFAULT_CLUSTER_TYPE = 'infrastructure'
ZK_TOPOLOGY_DIR = '/nail/etc/zookeeper_discovery'

LOG_FORMAT = '%(levelname)s %(message)s'
log = logging.getLogger()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--simulate', action='store_true',
                        help='Simluate only; do not actually delete any nodes.')
    parser.add_argument('-t', '--cluster-type', default=ZK_DEFAULT_CLUSTER_TYPE,
                        help='Cluster type (default: %(default)s).')
    return parser.parse_args()


def get_zk_cluster_locations(cluster_type):
    for path in glob.glob('%s/%s/*.yaml' % (ZK_TOPOLOGY_DIR, cluster_type)):
        if os.path.islink(path):
            # Ignore the 'local.yaml' symlink
            continue
        yield os.path.basename(path).replace('.yaml', '')


def get_zk_topology(cluster_type, cluster_location):
    zk_topology_path = os.path.join(
        ZK_TOPOLOGY_DIR, cluster_type, cluster_location + '.yaml'
    )
    with open(zk_topology_path) as fp:
        zk_topology = yaml.load(fp, Loader=Loader)
    return ['%s:%d' % (entry[0], entry[1]) for entry in zk_topology]


def clean(simulate, zk):
    removed_count = 0
    services = []

    try:
        locations = zk.get_children('/nerve')
    except kazoo.exceptions.NoNodeError:
        log.warn('No /nerve node found')

    for location in locations:
        services = zk.get_children('/nerve/%s' % location)
        for service in services:
            instances = zk.get_children('/nerve/%s/%s' % (location, service))
            for instance in instances:
                path = '/nerve/%s/%s/%s' % (location, service, instance)
                try:
                    data, stat = zk.get(path)
                except kazoo.exceptions.NoNodeError:
                    continue

                if len(data) != 0:
                    continue
                if stat.ephemeralOwner != 0:
                    continue
                if stat.numChildren != 0:
                    continue

                log.info('Removing %s' % path)
                removed_count += 1
                if not simulate:
                    zk.delete(path)

    return removed_count


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    logging.getLogger('kazoo').setLevel(logging.ERROR)

    args = parse_args()
    if args.simulate:
        log.info('Running in simulation mode')

    for cluster_location in get_zk_cluster_locations(args.cluster_type):
        log.info('Processing %s' % cluster_location)

        zk_topology = get_zk_topology(args.cluster_type, cluster_location)
        zk_hosts = ','.join(zk_topology)
        zk = kazoo.client.KazooClient(hosts=zk_hosts)

        try:
            zk.start()
        except:
            log.warn('Could not connect to zookeeper cluster for %s' % cluster_location)
            continue

        try:
            removed_count = clean(args.simulate, zk)
            log.info('Removed %d nodes' % removed_count)
        finally:
            zk.stop()


if __name__ == '__main__':
    main()
