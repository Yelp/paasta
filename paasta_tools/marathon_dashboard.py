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
from typing import Dict
from typing import List

from mypy_extensions import TypedDict

from paasta_tools.marathon_tools import get_marathon_clients
from paasta_tools.marathon_tools import get_marathon_servers
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.marathon_tools import MarathonClient
from paasta_tools.marathon_tools import MarathonClients
from paasta_tools.marathon_tools import MarathonServiceConfig
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
    for service_instance in instances:
        service: str = service_instance[0]
        instance: str = service_instance[1]
        service_config: MarathonServiceConfig = load_marathon_service_config(
            service=service,
            instance=instance,
            cluster=cluster,
            load_deployments=False,
            soa_dir=soa_dir,
        )
        client: MarathonClient = marathon_clients.get_current_client_for_service(job_config=service_config)
        dashboard_links: Dict = system_paasta_config.get_dashboard_links()
        shard_url: str = client.servers[0]
        if 'Marathon RO' in dashboard_links[cluster]:
            marathon_links = dashboard_links[cluster]['Marathon RO']
            if isinstance(marathon_links, list):
                for shard_number, shard in enumerate(marathon_servers.current):
                    if shard.url[0] == shard_url:
                        shard_url = marathon_links[shard_number]
            elif isinstance(marathon_links, str):
                shard_url = marathon_links.split(' ')[0]
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
