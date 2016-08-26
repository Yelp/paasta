#!/usr/bin/env python
"""Update the nerve configuration file and restart nerve if anything has
changed."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import filecmp
import json
import multiprocessing
import os
import os.path
import shutil
import signal
import socket
import subprocess
import sys
import time

import yaml
# When we build .debs of paasta, we make sure to depend on libyaml, so that CLoader is available. However, libyaml is
# not always available when paasta is installed as a python package (such as when running the unit tests with tox).
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


from environment_tools.type_utils import compare_types
from environment_tools.type_utils import convert_location_type
from environment_tools.type_utils import get_current_location
from paasta_tools.marathon_tools import get_services_running_here_for_nerve


# Used to determine the weight
try:
    CPUS = max(multiprocessing.cpu_count(), 10)
except NotImplementedError:
    CPUS = 10


def get_hostname():
    return socket.gethostname()


def get_ip_address():
    return socket.gethostbyname(get_hostname())


def get_named_zookeeper_topology(cluster_type, cluster_location, zk_topology_dir):
    """Use CEP 355 discovery to find zookeeper topologies"""
    zk_topology_path = os.path.join(
        zk_topology_dir, cluster_type, cluster_location + '.yaml'
    )
    with open(zk_topology_path) as fp:
        zk_topology = yaml.load(fp, Loader=Loader)
    return ['%s:%d' % (entry[0], entry[1]) for entry in zk_topology]


def generate_subconfiguration(service_name, advertise, extra_advertise, port,
                              ip_address, healthcheck_timeout_s, hacheck_uri, healthcheck_headers, hacheck_port,
                              weight, zk_topology_dir, zk_location_type, zk_cluster_type):
    config = {}

    # Register at the specified location types in the current superregion
    locations_to_register_in = set()
    for advertise_typ in advertise:
        locations_to_register_in.add((get_current_location(advertise_typ), advertise_typ))

    # Also register in any other locations specified in extra advertisements
    for (src, dst) in extra_advertise:
        src_typ, src_loc = src.split(':')
        dst_typ, dst_loc = dst.split(':')
        if get_current_location(src_typ) != src_loc:
            # We do not match the source
            continue
        # Convert the destination into the 'advertise' type(s)
        for advertise_typ in advertise:
            # Prevent upcasts, otherwise the service may be made available to
            # more hosts than intended.
            if compare_types(dst_typ, advertise_typ) > 0:
                continue
            for loc in convert_location_type(dst_loc, dst_typ, advertise_typ):
                locations_to_register_in.add((loc, advertise_typ))

    # Create a separate service entry for each location that we need to register in.
    for loc, typ in locations_to_register_in:
        zk_locations = convert_location_type(loc, typ, zk_location_type)
        for zk_location in zk_locations:
            try:
                zookeeper_topology = get_named_zookeeper_topology(
                    cluster_type=zk_cluster_type,
                    cluster_location=zk_location,
                    zk_topology_dir=zk_topology_dir,
                )
            except:
                continue

            key = '%s.%s.%s:%s.%d.new' % (
                service_name, zk_location, typ, loc, port
            )
            config[key] = {
                'port': port,
                'host': ip_address,
                'weight': weight,
                'zk_hosts': zookeeper_topology,
                'zk_path': '/nerve/%s:%s/%s' % (typ, loc, service_name),
                'check_interval': healthcheck_timeout_s + 1.0,
                # Hit the localhost hacheck instance
                'checks': [
                    {
                        'type': 'http',
                        'host': '127.0.0.1',
                        'port': hacheck_port,
                        'uri': hacheck_uri,
                        'timeout': healthcheck_timeout_s,
                        'open_timeout': healthcheck_timeout_s,
                        'rise': 1,
                        'fall': 2,
                        'headers': healthcheck_headers,
                    }
                ]
            }

    return config


def generate_configuration(services, heartbeat_path, hacheck_port, weight, zk_topology_dir, zk_location_type,
                           zk_cluster_type):
    nerve_config = {
        'instance_id': get_hostname(),
        'services': {},
        'heartbeat_path': heartbeat_path
    }

    ip_address = get_ip_address()

    for (service_name, service_info) in services:
        port = service_info.get('port')
        if port is None:
            continue

        mode = service_info.get('mode', 'http')
        healthcheck_timeout_s = service_info.get('healthcheck_timeout_s', 1.0)
        healthcheck_port = service_info.get('healthcheck_port', port)

        # hacheck will simply ignore the healthcheck_uri for TCP mode checks
        healthcheck_uri = service_info.get('healthcheck_uri', '/status')
        healthcheck_mode = service_info.get('healthcheck_mode', mode)
        hacheck_uri = '/%s/%s/%s/%s' % (
            healthcheck_mode, service_name, healthcheck_port, healthcheck_uri.lstrip('/'))
        advertise = service_info.get('advertise', ['region'])
        extra_advertise = service_info.get('extra_advertise', [])
        extra_healthcheck_headers = service_info.get('extra_healthcheck_headers', {})

        nerve_config['services'].update(
            generate_subconfiguration(
                service_name=service_name,
                advertise=advertise,
                extra_advertise=extra_advertise,
                port=port,
                ip_address=ip_address,
                healthcheck_timeout_s=healthcheck_timeout_s,
                hacheck_uri=hacheck_uri,
                healthcheck_headers=extra_healthcheck_headers,
                hacheck_port=hacheck_port,
                weight=weight,
                zk_topology_dir=zk_topology_dir,
                zk_location_type=zk_location_type,
                zk_cluster_type=zk_cluster_type,
            )
        )

    return nerve_config


def file_not_modified_since(path, threshold):
    """Returns true if a file has not been modified within some number of seconds

    :param path: a file path
    :param threshold: number of seconds
    :return: true if the file has not been modified within specified number of seconds, false otherwise
    """
    if os.path.isfile(path):
        return os.path.getmtime(path) < time.time() - threshold
    else:
        return False


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--heartbeat-path', default="/var/run/nerve/heartbeat",
                        help='path to nerve heartbeat file to monitor')
    parser.add_argument('-s', '--heartbeat-threshold', type=int, default=60,
                        help='if heartbeat file is not updated within this many seconds then nerve is restarted')
    parser.add_argument('--nerve-config-path', type=str, default='/etc/nerve/nerve.conf.json')
    parser.add_argument('--reload-with-sighup', action='store_true')
    parser.add_argument('--nerve-pid-path', type=str, default='/var/run/nerve.pid')
    parser.add_argument('--nerve-executable-path', type=str, default='/usr/bin/nerve')
    parser.add_argument('--nerve-backup-command', type=json.loads, default='["service", "nerve-backup"]')
    parser.add_argument('--nerve-command', type=json.loads, default='["service", "nerve"]')
    parser.add_argument('--nerve-registration-delay-s', type=int, default=30)
    parser.add_argument('--zk-topology-dir', type=str, default='/nail/etc/zookeeper_discovery')
    parser.add_argument('--zk-location-type', type=str, default='superregion',
                        help="What location type do the zookeepers live at?")
    parser.add_argument('--zk-cluster-type', type=str, default='infrastructure')
    parser.add_argument('--hacheck-port', type=int, default=6666)
    parser.add_argument('--weight', type=int, default=CPUS,
                        help='weight to advertise each service at. Defaults to # of CPUs')

    return parser.parse_args(args)


def main():
    opts = parse_args(sys.argv[1:])
    new_config = generate_configuration(
        services=get_services_running_here_for_nerve(),
        heartbeat_path=opts.heartbeat_path,
        hacheck_port=opts.hacheck_port,
        weight=opts.weight,
        zk_topology_dir=opts.zk_topology_dir,
        zk_location_type=opts.zk_location_type,
        zk_cluster_type=opts.zk_cluster_type,
    )

    # Must use os.rename on files in the same filesystem to ensure that
    # config is swapped atomically, so we need to create the temp file in
    # the same directory as the config file
    new_config_path = '{0}.tmp'.format(opts.nerve_config_path)

    with open(new_config_path, 'w') as fp:
        json.dump(new_config, fp, sort_keys=True, indent=4, separators=(',', ': '))

    # Match the permissions that puppet expects
    os.chmod(new_config_path, 0644)

    # Restart/reload nerve if the config files differ
    # Always force a restart if the heartbeat file is old
    should_reload = not filecmp.cmp(new_config_path, opts.nerve_config_path)
    should_restart = file_not_modified_since(opts.heartbeat_path, opts.heartbeat_threshold)

    # Always swap new config file into place, even if we're not going to
    # restart nerve. Our monitoring system checks the opts.nerve_config_path
    # file age to ensure that this script is functioning correctly.
    try:
        # Verify the new config is _valid_
        command = [opts.nerve_executable_path]
        command.extend(['-c', new_config_path, '-k'])
        subprocess.check_call(command)

        # Move the config over
        shutil.move(new_config_path, opts.nerve_config_path)
    except subprocess.CalledProcessError:
        # Nerve config is invalid!, bail out **without restarting**
        # so staleness monitoring can trigger and alert us of a problem
        return

    # If we can reload with SIGHUP, use that, otherwise use the normal
    # graceful method
    if should_reload and opts.reload_with_sighup:
        try:
            with open(opts.nerve_pid_path) as f:
                pid = int(f.read().strip())
            os.kill(pid, signal.SIGHUP)
        except (OSError, ValueError, IOError):
            # invalid pid file, time to restart
            should_restart = True
        else:
            # Always try to stop the backup process
            subprocess.call(opts.nerve_backup_command + ['stop'])
    else:
        should_restart |= should_reload

    if should_restart:
        # Try to do a graceful restart by starting up the backup nerve
        # prior to restarting the main nerve. Then once the main nerve
        # is restarted, stop the backup nerve.
        try:
            subprocess.call(opts.nerve_backup_command + ['start'])
            time.sleep(opts.nerve_registration_delay_s)

            subprocess.check_call(opts.nerve_command + ['stop'])
            subprocess.check_call(opts.nerve_command + ['start'])
            time.sleep(opts.nerve_registration_delay_s)
        finally:
            # Always try to stop the backup process
            subprocess.call(opts.nerve_backup_command + ['stop'])


if __name__ == '__main__':
    main()
