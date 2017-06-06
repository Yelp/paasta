# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import collections
import hashlib
import itertools
import json

import six

from paasta_tools import iptables
from paasta_tools.cli.utils import get_instance_config
from paasta_tools.marathon_tools import get_all_namespaces_for_service
from paasta_tools.utils import get_running_mesos_docker_containers
from paasta_tools.utils import load_system_paasta_config


PRIVATE_IP_RANGES = (
    '127.0.0.0/255.0.0.0',
    '10.0.0.0/255.0.0.0',
    '172.16.0.0/255.240.0.0',
    '192.168.0.0/255.255.0.0',
    '169.254.0.0/255.255.0.0',
)


class ServiceGroup(collections.namedtuple('ServiceGroup', (
    'service',
    'instance',
    'soa_dir',
))):
    """A service group.

    :param service: service name
    :param instance: instance name
    :param soa_dir: path to yelpsoa-configs
    """
    __slots__ = ()

    @property
    def chain_name(self):
        """Return iptables chain name.

        Chain names are limited to 28 characters, so we have to trim quite a
        bit. To attempt to ensure we don't have collisions due to shortening,
        we append a hash to the end.
        """
        chain = 'PAASTA.{}'.format(self.service[:10])
        chain += '.' + hashlib.sha256(
            json.dumps(self).encode('utf8'),
        ).hexdigest()[:10]
        assert len(chain) <= 28, len(chain)
        return chain

    @property
    def config(self):
        return get_instance_config(
            self.service, self.instance,
            load_system_paasta_config().get_cluster(),
            load_deployments=False,
            soa_dir=self.soa_dir,
        )

    @property
    def rules(self):
        conf = self.config

        if conf.get_dependencies() is None:
            return ()

        rules = [_default_rule(conf)]
        rules.extend(_well_known_rules(conf))
        rules.extend(_smartstack_rules(conf, self.soa_dir))
        return tuple(rules)

    def update_rules(self):
        iptables.ensure_chain(self.chain_name, self.rules)

    @property
    def log_prefix(self):
        # log-prefix is limited to 29 characters total
        # space at the end is necessary to separate it from the rest of the line
        # no restrictions on any particular characters afaict
        return 'paasta.{}'.format(self.service)[:28] + ' '


def _default_rule(conf):
    policy = conf.get_outbound_firewall()
    if policy == 'block':
        return iptables.Rule(
            protocol='ip',
            src='0.0.0.0/0.0.0.0',
            dst='0.0.0.0/0.0.0.0',
            target='REJECT',
            matches=(),
            target_parameters=(
                (('reject-with', ('icmp-port-unreachable',))),
            ),
        )
    elif policy == 'monitor':
        return iptables.Rule(
            protocol='ip',
            src='0.0.0.0/0.0.0.0',
            dst='0.0.0.0/0.0.0.0',
            target='LOG',
            target_parameters=(
                ('log-prefix', ((conf.log_prefix),)),
            ),
            matches=(
                ('limit', (('limit', '1/sec'),)),
            )
        )
    else:
        raise AssertionError(policy)


def _well_known_rules(conf):
    for dep in conf.get_dependencies():
        resource = dep.get('well-known')
        if resource == 'internet':
            yield iptables.Rule(
                protocol='ip',
                src='0.0.0.0/0.0.0.0',
                dst='0.0.0.0/0.0.0.0',
                target='PAASTA-INTERNET',
                matches=(),
                target_parameters=(),
            )
        elif resource is not None:
            # TODO: handle better
            raise AssertionError(resource)


def _smartstack_rules(conf, soa_dir):
    for dep in conf.get_dependencies():
        namespace = dep.get('smartstack')
        if namespace is None:
            continue

        # TODO: handle non-synapse-haproxy services
        # TODO: support wildcards?
        service, _ = namespace.split('.', 1)
        service_namespaces = get_all_namespaces_for_service(service, soa_dir=soa_dir)
        port = dict(service_namespaces)[namespace]['proxy_port']

        yield iptables.Rule(
            protocol='tcp',
            src='0.0.0.0/0.0.0.0',
            dst='169.254.255.254/255.255.255.255',
            target='ACCEPT',
            matches=(
                ('tcp', (('dport', six.text_type(port)),)),
            ),
            target_parameters=(),
        )


def services_running_here():
    """Generator helper that yields (service, instance, mac address) of both
    marathon and chronos tasks.
    """
    for container in get_running_mesos_docker_containers():
        if container['HostConfig']['NetworkMode'] != 'bridge':
            continue

        service = container['Labels'].get('paasta_service')
        instance = container['Labels'].get('paasta_instance')

        if service is None or instance is None:
            continue

        network_info = container['NetworkSettings']['Networks']['bridge']

        mac = network_info['MacAddress']
        ip = network_info['IPAddress']
        yield service, instance, mac, ip


def active_service_groups(soa_dir):
    """Return active service groups."""
    service_groups = collections.defaultdict(set)
    for service, instance, mac, ip in services_running_here():
        service_groups[ServiceGroup(service, instance, soa_dir)].add(mac)
    return service_groups


def ensure_internet_chain():
    iptables.ensure_chain(
        'PAASTA-INTERNET',
        (
            iptables.Rule(
                protocol='ip',
                src='0.0.0.0/0.0.0.0',
                dst='0.0.0.0/0.0.0.0',
                target='ACCEPT',
                matches=(),
                target_parameters=(),
            ),
        ) + tuple(
            iptables.Rule(
                protocol='ip',
                src='0.0.0.0/0.0.0.0',
                dst=ip_range,
                target='RETURN',
                matches=(),
                target_parameters=(),
            )
            for ip_range in PRIVATE_IP_RANGES
        )
    )


def ensure_service_chains(soa_dir):
    """Ensure service chains exist and have the right rules.

    Returns dictionary {[service chain] => [list of mac addresses]}.
    """
    chains = {}
    for service, macs in active_service_groups(soa_dir).items():
        service.update_rules()
        chains[service.chain_name] = macs
    return chains


def ensure_dispatch_chains(service_chains):
    paasta_rules = set(itertools.chain.from_iterable(
        (
            iptables.Rule(
                protocol='ip',
                src='0.0.0.0/0.0.0.0',
                dst='0.0.0.0/0.0.0.0',
                target=chain,
                matches=(
                    ('mac', (('mac_source', mac.upper()),)),
                ),
                target_parameters=(),
            )
            for mac in macs
        )
        for chain, macs in service_chains.items()

    ))
    iptables.ensure_chain('PAASTA', paasta_rules)

    jump_to_paasta = iptables.Rule(
        protocol='ip',
        src='0.0.0.0/0.0.0.0',
        dst='0.0.0.0/0.0.0.0',
        target='PAASTA',
        matches=(),
        target_parameters=(),
    )
    iptables.ensure_rule('INPUT', jump_to_paasta)
    iptables.ensure_rule('FORWARD', jump_to_paasta)


def garbage_collect_old_service_chains(desired_chains):
    current_paasta_chains = {
        chain
        for chain in iptables.all_chains()
        if chain.startswith('PAASTA.')
    }
    for chain in current_paasta_chains - set(desired_chains):
        iptables.delete_chain(chain)


def general_update(soa_dir):
    """Update iptables to match the current PaaSTA state."""
    ensure_internet_chain()
    service_chains = ensure_service_chains(soa_dir)
    ensure_dispatch_chains(service_chains)
    garbage_collect_old_service_chains(service_chains)
