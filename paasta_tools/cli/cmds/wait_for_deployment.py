#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Contains methods used by the paasta client to mark a docker image for
deployment to a cluster.instance.
"""
import sys
import time


from paasta_tools import remote_git
from paasta_tools.api import client
from paasta_tools.cli.utils import calculate_remote_masters
from paasta_tools.cli.utils import figure_out_service_name
from paasta_tools.cli.utils import lazy_choices_completer
from paasta_tools.cli.utils import list_services
from paasta_tools.cli.utils import validate_service_name
from paasta_tools.utils import _log
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import format_tag
from paasta_tools.utils import get_paasta_tag_from_deploy_group
from paasta_tools.utils import list_clusters
from paasta_tools.utils import load_system_paasta_config

def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'wait-for-deployment',
        help='Block until a service is ready to serve traffic',
        description=(
            "'paasta wait-for-deployment' uses the PaaSTA API to get the status"
            " of a certain service and exists either if the service is ready or "
            " if a timeout has been reached."
        )
    )
    list_parser.add_argument(
        '-s', '--service',
        help='The name of the service you wish to inspect'
    ).completer = lazy_choices_completer(list_services)
    list_parser.add_argument(
        '-c', '--clusters',
        help="A comma-separated list of clusters to view. Defaults to view all clusters.\n"
             "For example: --clusters norcal-prod,nova-prod",
        required=True
    ).completer = lazy_choices_completer(list_clusters)
    list_parser.add_argument(
        '-i', '--instances',
        help="A comma-separated list of instances to view. Defaults to view all instances.\n"
             "For example: --instances canary,main",
        required=True
    )  # No completer because we need to know service first and we can't until some other stuff has happened
    list_parser.add_argument(
        '-d', '--soa-dir',
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    list_parser.add_argument(
        '-t', '--interval',
        help="Interval between check attempts",
        type=int,
        default=5
    )
    list_parser.add_argument(
        '-a', '--attempts',
        help="Number of attempts",
        type=int
    )
    list_parser.set_defaults(command=paasta_wait_for_deployment)


def get_instance_status(cluster, service, instance):
    paasta_client = client.PaastaClient()
    return paasta_client.status(cluster, service, instance)

def wait_for_deployment(check_map, attempts=None, interval=5):
    to_check = []
    completed = []
    failed = []
    cluster_map = {}
    for service, svc_data in check_map.items():
        for cluster in svc_data['clusters']:
            for instance in svc_data['instances']:
                to_check.append((cluster, service, instance))

    cur_attempts = 0
    while True:
        cur_attempts += 1
        for i_name in to_check:
            if i_name in failed:
                continue
            cluster_name, service, instance = i_name
            print 'Checking {0}.{1} at {2}'.format(service, instance, cluster_name)
            i_data = get_instance_status(cluster_name, service, instance)
            if not i_data:
                print '\tCheck failed.'
                failed.append(i_name)
                continue
            try:
                cur_status = i_data['marathon']['deploy_status']
                if cur_status == 'Running':
                    expected_count = i_data['marathon']['expected_instance_count']
                    running_count = i_data['marathon']['running_instance_count']
                    if i_data['marathon']['desired_state'] == 'start' \
                            and expected_count ==  running_count:
                        completed.append(i_name)
                        print '\tCheck Successful!'
            except KeyError, e:
                print e
                failed.append(i_name)
        if attempts and cur_attempts == attempts:
            print 'Timeout reached after {0} seconds. Aborting'.format(
                    attempts*interval)
            sys.exit(2)
        if (len(completed) + len(failed)) == len(to_check):
            break
        else:
            print 'Sleeping, Completed: {0} out of {1}'.format(
                    (len(completed) + len(failed)), len(to_check))
            time.sleep(interval)

    if not len(failed):
        print '\nAll checks were successful'
        sys.exit(0)
    else:
        print '\nSome Checks failed'
        sys.exit(1)


def paasta_wait_for_deployment(args):
    """Wrapping wait_for_deployment"""
    soa_dir = args.soa_dir
    service = figure_out_service_name(args, soa_dir)
    system_paasta_config = load_system_paasta_config()
    check_map = {service: {}}
    cluster_list = args.clusters.split(",")
    fqdn_format = system_paasta_config.get_cluster_fqdn_format()
    ssl_enabled = system_paasta_config.get_api_ssl()
    cluster_endpoints = system_paasta_config.get_api_endpoints()
    proto = 'https' if ssl_enabled else 'http'
    check_map[service]['clusters'] = cluster_list
    instance_whitelist = args.instances.split(",")
    check_map[service]['instances'] = instance_whitelist

    return wait_for_deployment(check_map, attempts=args.attempts,
            interval=args.interval)
