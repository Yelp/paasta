#!/usr/bin/env python3
import argparse
import asyncio
import functools
import json
import logging
import os.path
import re
import socket
import sys
from collections import defaultdict
from enum import Enum
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
from kazoo.exceptions import NoNodeError

logger = logging.getLogger("check_orphans")

PREFIX = "/smartstack/global/"
CHUNK_SIZE = 50  # How many concurrent xinetd connections
DEFAULT_ZK_DISCOVERY_PATH = "/nail/etc/zookeeper_discovery/infrastructure/local.yaml"
DEFAULT_NERVE_XINETD_PORT = 8735


class ExitCode(Enum):
    OK = 0
    ORPHANS = 1
    COLLISIONS = 2


def get_zk_hosts(path: str) -> List[str]:
    with open(path) as f:
        x = yaml.safe_load(f)
    return [f"{host}:{port}" for host, port in x]


SmartstackData = Dict[str, Dict[str, Any]]


def get_zk_data(ignored_services: Set[str]) -> SmartstackData:
    logger.info(f"using {DEFAULT_ZK_DISCOVERY_PATH} for zookeeper")
    zk_hosts = get_zk_hosts(DEFAULT_ZK_DISCOVERY_PATH)

    logger.debug(f"connecting to zk hosts {zk_hosts}")
    zk = KazooClient(hosts=zk_hosts)
    zk.start()

    logger.debug(f"pulling smartstack data from zookeeper")
    zk_data = {}
    services = zk.get_children(PREFIX)
    for service in services:
        if service in ignored_services:
            continue
        service_instances = zk.get_children(os.path.join(PREFIX, service))
        instances_data = {}
        for instance in service_instances:
            try:
                instance_node = zk.get(os.path.join(PREFIX, service, instance))
            except NoNodeError:
                continue
            instances_data[instance] = json.loads(instance_node[0])
            zk_data[service] = instances_data

    return zk_data


class InstanceTuple(NamedTuple):
    # paasta_host may be different from the service's host if running on k8s.
    # We need the actual PaaSTA host because the k8s pod does not listen for
    # xinetd connections.
    paasta_host: str
    host: str
    port: int
    service: str


def read_from_zk_data(registrations: SmartstackData) -> Set[InstanceTuple]:
    return {
        InstanceTuple(
            host_to_ip(instance_data["name"], instance_data["host"]),
            instance_data["host"],
            instance_data["port"],
            service,
        )
        for service, instance in registrations.items()
        for instance_data in instance.values()
    }


@functools.lru_cache()
def host_to_ip(host: str, fallback: str) -> str:
    """Try to resolve a host to an IP with a fallback.

    Because DNS resolution is relatively slow and can't be easily performed
    using asyncio, we cheat a little and use a regex for well-formed hostnames
    to try to guess the IP without doing real resolution.

    A fallback is needed because in some cases the nerve registration does not
    match an actual hostname (e.g. "prod-db15" or "prod-splunk-master").
    """
    for match in (
        re.match(r"^(\d+)-(\d+)-(\d+)-(\d+)-", host),
        re.match(r"^ip-(\d+)-(\d+)-(\d+)-(\d+)", host),
    ):
        if match:
            return ".".join(match.groups())
    else:
        try:
            return socket.gethostbyname(host)
        except socket.gaierror:
            return fallback


async def transfer_one_file(
    host: str, port: int = DEFAULT_NERVE_XINETD_PORT
) -> Tuple[str, Optional[str]]:
    logger.debug(f"getting file from {host}")
    try:
        reader, _ = await asyncio.wait_for(
            asyncio.open_connection(host=host, port=port, limit=2 ** 32), timeout=1.0
        )
        resp = await asyncio.wait_for(reader.read(), timeout=1.0)
    except (asyncio.TimeoutError, ConnectionRefusedError) as ex:
        # this is not ununusual because we sometimes advertise hosts from
        # firewalled subnets where we can't make this connection to get
        # the file. check y/ipam to see what the subnet means
        logger.debug(f"error getting file from {host}: {ex!r}")
        return (host, None)

    return (host, resp.decode())


async def gather_files(hosts: Set[str]) -> Dict[str, str]:
    logger.info("gathering files from {} hosts".format(len(hosts)))
    tasks = [transfer_one_file(host) for host in hosts]
    responses = {}
    for idx in range(0, len(tasks), CHUNK_SIZE):
        resp = await asyncio.gather(
            *tasks[idx : idx + CHUNK_SIZE], return_exceptions=True
        )
        responses.update(dict(resp))
    return responses


def read_one_nerve_file(nerve_config: str) -> Set[InstanceTuple]:
    nerve_config = json.loads(nerve_config)
    return {
        InstanceTuple(
            # The "instance_id" configured in nerve's config file is the same
            # as the "name" attribute in a zookeeper registration (i.e. for
            # PaaSTA hosts, it will be the hostname of the machine running
            # nerve). To be able to easily compare the tuples using set
            # operations, we resolve it to an IP in both places.
            host_to_ip(nerve_config["instance_id"], service["host"]),
            service["host"],
            service["port"],
            service["zk_path"][len(PREFIX) :],
        )
        for service in nerve_config["services"].values()
        if service["zk_path"].startswith(PREFIX)
    }


def read_nerve_files(
    nerve_configs: Dict[str, Optional[str]]
) -> Tuple[Set[InstanceTuple], Set[str]]:
    instance_set: Set[InstanceTuple] = set()
    not_found_hosts: Set[str] = set()
    for host, host_config in nerve_configs.items():
        if host_config is None:
            not_found_hosts.add(host)
        else:
            instance_set |= read_one_nerve_file(host_config)
    return instance_set, not_found_hosts


def get_instance_data(
    ignored_services: Set[str],
) -> Tuple[Set[InstanceTuple], Set[InstanceTuple]]:
    # Dump ZK
    zk_data = get_zk_data(ignored_services)
    zk_instance_data = read_from_zk_data(zk_data)

    hosts = {x[0] for x in zk_instance_data}

    # Dump Nerve configs from each host via xinetd
    results = asyncio.get_event_loop().run_until_complete(gather_files(hosts))

    nerve_instance_data, not_found_hosts = read_nerve_files(results)

    # Filter out anything that we couldn't get a nerve config for
    zk_instance_data_filtered = {
        x for x in zk_instance_data if x[0] not in not_found_hosts
    }

    logger.info("zk_instance_data (unfiltered) len: {}".format(len(zk_instance_data)))
    logger.info(
        "zk_instance_data (filtered) len: {}".format(len(zk_instance_data_filtered))
    )
    logger.info("nerve_instance_data len: {}".format(len(nerve_instance_data)))

    return zk_instance_data_filtered, nerve_instance_data


def check_orphans(
    zk_instance_data: Set[InstanceTuple], nerve_instance_data: Set[InstanceTuple]
) -> ExitCode:
    orphans = zk_instance_data - nerve_instance_data

    # groupby host
    orphans_by_host: DefaultDict[str, List[Tuple[int, str]]] = defaultdict(list)
    for orphan in orphans:
        orphans_by_host[orphan.host].append((orphan.port, orphan.service))

    # collisions
    instance_by_addr: DefaultDict[Tuple[str, int], Set[str]] = defaultdict(set)
    for nerve_inst in nerve_instance_data:
        instance_by_addr[(nerve_inst.host, nerve_inst.port)].add(nerve_inst.service)

    collisions: List[str] = []
    for zk_inst in zk_instance_data:
        nerve_services = instance_by_addr[(zk_inst.host, zk_inst.port)]
        if len(nerve_services) >= 1 and zk_inst.service not in nerve_services:
            collisions.append(
                f"[{zk_inst.host}:{zk_inst.port}] {zk_inst.service} collides with {nerve_services}"
            )

    if collisions:
        logger.warning("Collisions found! Traffic is being misrouted!")
        print("\n".join(collisions))
        return ExitCode.COLLISIONS
    elif orphans:
        logger.warning("{} orphans found".format(len(orphans)))
        print(dict(orphans_by_host))
        return ExitCode.ORPHANS
    else:
        logger.info(
            "No orphans found out of {} service registrations seen".format(
                len(zk_instance_data)
            )
        )
        return ExitCode.OK


def main() -> ExitCode:
    logging.basicConfig(level=logging.WARNING)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ignored-services",
        # TODO(ckuehl|2020-08-27): Remove this deprecated option alias eventually.
        "--blacklisted-services-DEPRECATED",
        default="",
        type=str,
        help="Comma separated list of services to ignore",
    )
    args = parser.parse_args()

    zk_instance_data, nerve_instance_data = get_instance_data(
        set(args.ignored_services.split(","))
    )

    return check_orphans(zk_instance_data, nerve_instance_data)


if __name__ == "__main__":
    sys.exit(main().value)
