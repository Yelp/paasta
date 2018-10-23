#!/usr/bin/env python
# Copyright 2017 Yelp Inc.
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
import argparse
import json
import logging
from collections import defaultdict
from typing import Dict
from typing import List
from typing import Set

from mypy_extensions import TypedDict

from paasta_tools.marathon_tools import get_marathon_clients
from paasta_tools.marathon_tools import get_marathon_servers
from paasta_tools.marathon_tools import MarathonClient
from paasta_tools.marathon_tools import MarathonClients
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_services_for_cluster
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import SystemPaastaConfig


log = logging.getLogger(__name__)


def parse_args(argv) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Generates links from marathon instances to their respective web dashboard.',
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
    args = parser.parse_args(argv)
    return args


Marathon_Dashboard_Item = TypedDict('Marathon_Dashboard_Item', {'service': str, 'instance': str, 'shard_url': str})
Marathon_Dashboard = Dict[str, List[Marathon_Dashboard_Item]]


def create_marathon_dashboard(
        cluster: str,
        soa_dir: str=DEFAULT_SOA_DIR,
        marathon_clients: MarathonClients=None,
        system_paasta_config: SystemPaastaConfig=None,
) -> Marathon_Dashboard:
    try:
        instances: List = get_services_for_cluster(
            cluster=cluster,
            instance_type='marathon',
            soa_dir=soa_dir,
        )
    except FileNotFoundError:
        instances = []
    dashboard: Marathon_Dashboard = {cluster: []}
    if system_paasta_config is None:
        system_paasta_config = load_system_paasta_config()
    marathon_servers = get_marathon_servers(system_paasta_config=system_paasta_config)
    if marathon_clients is None:
        marathon_clients = get_marathon_clients(marathon_servers=marathon_servers, cached=False)

    dashboard_links = system_paasta_config.get_dashboard_links()
    marathon_links = dashboard_links.get(cluster, {}).get('Marathon RO')

    # e.g. 'http://10.64.97.75:5052': 'http://marathon-norcal-prod.yelpcorp.com'
    shard_url_to_marathon_link_dict: Dict[str, str] = {}
    if isinstance(marathon_links, list):
        # Sanity check and log error if necessary
        if len(marathon_links) != len(marathon_servers.current):
            log.error('len(marathon_links) != len(marathon_servers.current). This may be a cause of concern')
        for shard_number, shard in enumerate(marathon_servers.current):
            shard_url_to_marathon_link_dict[shard.url[0]] = marathon_links[shard_number]
    elif isinstance(marathon_links, str):
        # In this case, the shard url will be the same for every service instance
        static_shard_url = marathon_links.split(' ')[0]
        return {cluster: [{'service': si[0], 'instance': si[1], 'shard_url': static_shard_url} for si in instances]}

    # Setup with service as key since will instantiate 1 PSCL per service
    service_instances_dict: Dict[str, Set[str]] = defaultdict(set)
    for si in instances:
        service, instance = si[0], si[1]
        service_instances_dict[service].add(instance)

    for service, instance_set in service_instances_dict.items():
        pscl = PaastaServiceConfigLoader(
            service=service,
            soa_dir=soa_dir,
            load_deployments=False,
        )
        for marathon_service_config in pscl.instance_configs(cluster, MarathonServiceConfig):
            if marathon_service_config.get_instance() in instance_set:
                client: MarathonClient = \
                    marathon_clients.get_current_client_for_service(job_config=marathon_service_config)
                ip_url: str = client.servers[0]
                # Convert to a marathon link if possible else default to the originalIP address
                shard_url: str = shard_url_to_marathon_link_dict.get(ip_url, ip_url)
                service_info: Marathon_Dashboard_Item = {
                    'service': service,
                    'instance': instance,
                    'shard_url': shard_url,
                }
                dashboard[cluster].append(service_info)
    return dashboard


def main(argv=None) -> None:
    args = parse_args(argv)
    dashboard: Marathon_Dashboard = create_marathon_dashboard(cluster=args.cluster, soa_dir=args.soa_dir)
    print(json.dumps(dashboard))


if __name__ == "__main__":
    main()
