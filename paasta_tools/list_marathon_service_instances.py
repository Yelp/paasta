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
"""Usage: ./list_marathon_service_instances.py [options]

Enumerates all marathon instances for services in the soa directory that
are for the current cluster (defined by the marathon configuration file).

Outputs (to stdout) a list of service.instance (one per line)
for each instance found in marathon-<CLUSTER>.yaml for every folder
in the SOA Configuration directory.

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -c <CLUSTER>, --cluster <CLUSTER>: Specify which cluster of services to read
- -m, --minimal: Only show service instances that need bouncing
"""
import argparse
import sys

from paasta_tools.marathon_tools import DEFAULT_SOA_DIR
from paasta_tools.marathon_tools import get_marathon_apps_with_clients
from paasta_tools.marathon_tools import get_marathon_clients
from paasta_tools.marathon_tools import get_marathon_servers
from paasta_tools.marathon_tools import get_num_at_risk_tasks
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.mesos_maintenance import get_draining_hosts
from paasta_tools.utils import _log
from paasta_tools.utils import compose_job_id
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import long_job_id_to_short_job_id
from paasta_tools.utils import paasta_print
from paasta_tools.utils import use_requests_cache


def parse_args():
    parser = argparse.ArgumentParser(
        description='Lists marathon instances for a service.',
    )
    parser.add_argument(
        '-c', '--cluster', dest="cluster", metavar="CLUSTER",
        default=None,
        help="define a specific cluster to read from",
    )
    parser.add_argument(
        '-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        '-m', '--minimal', dest='minimal', action='store_true',
        help="show only service instances that need bouncing",
    )
    args = parser.parse_args()
    return args


def get_desired_marathon_configs(soa_dir):
    cluster = load_system_paasta_config().get_cluster()
    instances = get_services_for_cluster(
        instance_type='marathon',
        cluster=cluster,
        soa_dir=soa_dir,
    )

    job_configs = dict()
    formatted_marathon_configs = dict()

    for service, instance in instances:
        try:
            job_config = load_marathon_service_config(
                service=service,
                instance=instance,
                cluster=cluster,
                soa_dir=soa_dir,
            )

            formatted_config = job_config.format_marathon_app_dict()
            formatted_marathon_configs[formatted_config['id'].lstrip('/')] = formatted_config
            job_configs[formatted_config['id'].lstrip('/')] = job_config
        # Not ideal but we rely on a lot of user input to create the app dict
        # and we really can't afford to bail if just one app definition is malformed
        except Exception as errormsg:
            _log(
                service=service,
                line=str(errormsg),
                component='deploy',
                level='debug',
                cluster=cluster,
                instance=instance,
            )
    return formatted_marathon_configs, job_configs


@use_requests_cache('list_marathon_services')
def get_service_instances_that_need_bouncing(marathon_clients, soa_dir):
    desired_marathon_configs_formatted, desired_job_configs = get_desired_marathon_configs(soa_dir)
    desired_ids_and_clients = set()
    for app_id, job_config in desired_job_configs.items():
        desired_ids_and_clients.add((app_id, marathon_clients.get_current_client_for_service(job_config)))

    current_apps_with_clients = {
        (app.id.lstrip('/'), client): app
        for app, client in get_marathon_apps_with_clients(marathon_clients.get_all_clients())
    }
    actual_ids_and_clients = set(current_apps_with_clients.keys())

    undesired_apps_and_clients = actual_ids_and_clients.symmetric_difference(desired_ids_and_clients)
    apps_that_need_bouncing = {long_job_id_to_short_job_id(app_id) for app_id, client in undesired_apps_and_clients}

    draining_hosts = get_draining_hosts()

    for (app_id, client), app in current_apps_with_clients.items():
        short_app_id = long_job_id_to_short_job_id(app_id)
        if short_app_id not in apps_that_need_bouncing:
            if (
                app.instances != desired_marathon_configs_formatted[app_id]['instances'] or
                get_num_at_risk_tasks(app, draining_hosts) != 0
            ):
                apps_that_need_bouncing.add(short_app_id)

    return (app_id.replace('--', '_') for app_id in apps_that_need_bouncing)


def main():
    args = parse_args()
    soa_dir = args.soa_dir
    cluster = args.cluster
    if args.minimal:
        system_paasta_config = load_system_paasta_config()
        marathon_servers = get_marathon_servers(system_paasta_config)
        marathon_clients = get_marathon_clients(marathon_servers)
        service_instances = get_service_instances_that_need_bouncing(
            marathon_clients=marathon_clients, soa_dir=soa_dir,
        )
    else:
        instances = get_services_for_cluster(
            cluster=cluster,
            instance_type='marathon',
            soa_dir=soa_dir,
        )
        service_instances = []
        for name, instance in instances:
            service_instances.append(compose_job_id(name, instance))
    paasta_print('\n'.join(service_instances))
    sys.exit(0)


if __name__ == "__main__":
    main()
