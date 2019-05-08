#!/usr/bin/env python3
import argparse
import asyncio
import json
import logging
import os.path
import pathlib
from collections import defaultdict
from typing import Any
from typing import DefaultDict
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Set
from typing import Tuple

import yaml
from kazoo.client import KazooClient

PREFIX = '/smartstack/global/'
CHUNK_SIZE = 50  # How many concurrent xinetd connections
DEFAULT_ZK_DISCOVERY_PATH = '/nail/etc/zookeeper_discovery/infrastructure/local.yaml'
DEFAULT_NERVE_XINETD_PORT = 8735
DEFAULT_SERVICE_BLACKLIST_PATH = './orphan_service_blacklist.yaml'
DEFAULT_OUTPUT_DIR = './output/'


def get_zk_hosts(path: str) -> List[str]:
    with open(path) as f:
        x = yaml.load(f)
    return [f'{host}:{port}' for host, port in x]


SmartstackData = Dict[str, Dict[str, Any]]


def get_zk_data(blacklisted_services: Set[str]) -> SmartstackData:
    logging.info(f'using {DEFAULT_ZK_DISCOVERY_PATH} for zookeeper')
    zk_hosts = get_zk_hosts(DEFAULT_ZK_DISCOVERY_PATH)

    logging.debug(f'connecting to zk hosts {zk_hosts}')
    zk = KazooClient(hosts=zk_hosts)
    zk.start()

    logging.debug(f'pulling smartstack data from zookeeper')
    zk_data = {}
    services = zk.get_children(PREFIX)
    for service in services:
        if service in blacklisted_services:
            continue
        service_instances = zk.get_children(os.path.join(PREFIX, service))
        instances_data = {}
        for instance in service_instances:
            instance_node = zk.get(os.path.join(PREFIX, service, instance))
            instances_data[instance] = json.loads(instance_node[0])
            zk_data[service] = instances_data

    return zk_data


class InstanceTuple(NamedTuple):
    host: str
    port: int
    service: str


def read_from_zk_data(registrations: SmartstackData) -> Set[InstanceTuple]:
    return {
        InstanceTuple(instance_data['host'], instance_data['port'], service)
        for service, instance in registrations.items()
        for instance_data in instance.values()
    }


async def transfer_one_file(host: str, port: int = DEFAULT_NERVE_XINETD_PORT) -> Tuple[str, Optional[str]]:
    logging.debug(f'getting file from {host}')
    try:
        reader, _ = await asyncio.wait_for(
            asyncio.open_connection(host=host, port=port, limit=2**32),
            timeout=1.0,
        )
        resp = await asyncio.wait_for(reader.read(), timeout=1.0)
    except (asyncio.TimeoutError, ConnectionRefusedError):
        logging.error(f'error getting file from {host}')
        return (host, None)

    return (host, resp.decode())


async def gather_files(hosts: Set[str]) -> Dict[str, str]:
    logging.info('gathering files from {} hosts'.format(len(hosts)))
    tasks = [transfer_one_file(host) for host in hosts]
    responses = {}
    for idx in range(0, len(tasks), CHUNK_SIZE):
        resp = await asyncio.gather(*tasks[idx:idx + CHUNK_SIZE], return_exceptions=True)
        responses.update(dict(resp))
    return responses


def read_one_nerve_file(nerve_config: str) -> Set[InstanceTuple]:
    services = json.loads(nerve_config)['services']
    return {
        InstanceTuple(service['host'], service['port'], service['zk_path'][len(PREFIX):])
        for service in services.values()
        if service['zk_path'].startswith(PREFIX)
    }


def read_nerve_files(nerve_configs: Dict[str, Optional[str]]) -> Tuple[Set[InstanceTuple], Set[str]]:
    instance_set: Set[InstanceTuple] = set()
    not_found_hosts: Set[str] = set()
    for host, host_config in nerve_configs.items():
        if host_config is None:
            not_found_hosts.add(host)
        else:
            instance_set |= read_one_nerve_file(host_config)
    return instance_set, not_found_hosts


def get_instance_data(service_blacklist_path: str) -> Tuple[Set[InstanceTuple], Set[InstanceTuple]]:
    blacklisted_services = set(yaml.load(service_blacklist_path))
    # Dump ZK
    zk_data = get_zk_data(blacklisted_services)
    zk_instance_data = read_from_zk_data(zk_data)

    hosts = {x[0] for x in zk_instance_data}

    # Dump Nerve configs from each host via xinetd
    coro = gather_files(hosts)
    results = asyncio.get_event_loop().run_until_complete(coro)

    nerve_instance_data, not_found_hosts = read_nerve_files(results)

    # Filter out anything that we couldn't get a nerve config for
    zk_instance_data_filtered = {x for x in zk_instance_data if x[0] not in not_found_hosts}

    logging.info('zk_instance_data (unfiltered) len: {}'.format(len(zk_instance_data)))
    logging.info('zk_instance_data (filtered) len: {}'.format(len(zk_instance_data_filtered)))
    logging.info('nerve_instance_data len: {}'.format(len(nerve_instance_data)))

    return zk_instance_data_filtered, nerve_instance_data


def write_output(
    output_dir: str,
    zk_instance_data: Set[InstanceTuple],
    nerve_instance_data: Set[InstanceTuple],
) -> None:
    orphans = zk_instance_data - nerve_instance_data
    if orphans:
        logging.warning('{} orphans found'.format(len(orphans)))

    pathlib.Path(output_dir).mkdir(exist_ok=True)

    # groupby host

    orphans_by_host: DefaultDict[str, List[Tuple[int, str]]] = defaultdict(list)
    for orphan in orphans:
        orphans_by_host[orphan.host].append((orphan.port, orphan.service))
    with open(os.path.join(output_dir, 'orphans_by_host.yaml'), 'w') as f:
        yaml.dump(dict(orphans_by_host), f)

    # groupby service
    orphans_by_service: DefaultDict[str, List[Tuple[str, int]]] = defaultdict(list)
    for orphan in orphans:
        orphans_by_service[orphan.service].append((orphan.host, orphan.port))
    with open(os.path.join(output_dir, 'orphans_by_service.yaml'), 'w') as f:
        yaml.dump(dict(orphans_by_service), f)

    # collisions
    instance_by_addr: DefaultDict[Tuple[str, int], Set[str]] = defaultdict(set)
    for nerve_inst in nerve_instance_data:
        instance_by_addr[(nerve_inst.host, nerve_inst.port)].add(nerve_inst.service)

    collisions: List[str] = []
    for zk_inst in zk_instance_data:
        nerve_services = instance_by_addr[(zk_inst.host, zk_inst.port)]
        if len(nerve_services) >= 1 and zk_inst.service not in nerve_services:
            collisions.append(
                f'[{zk_inst.host}:{zk_inst.port}] {zk_inst.service} collides with {nerve_services}',
            )

    if collisions:
        logging.warning('Collisions found! Traffic is being misrouted!')
    with open(os.path.join(output_dir, 'collisions.yaml'), 'w') as f:
        f.write('\n'.join(collisions))

    logging.info('Wrote out orphan data')


def main():
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--service-blacklist-path',
        default=DEFAULT_SERVICE_BLACKLIST_PATH,
        help="Path to a file that contains services to ignore",
    )
    parser.add_argument(
        '--output-dir',
        default=DEFAULT_OUTPUT_DIR,
        help="Where to write the output files",
    )
    args = parser.parse_args()

    zk_instance_data, nerve_instance_data = get_instance_data(args.service_blacklist_path)

    write_output(args.output_dir, zk_instance_data, nerve_instance_data)


if __name__ == '__main__':
    main()
