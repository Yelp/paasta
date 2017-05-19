# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import collections
import hashlib
import itertools
import json

from paasta_tools import iptables


PRIVATE_IP_RANGES = (
    '127.0.0.0/255.0.0.0',
    '10.0.0.0/255.0.0.0',
    '172.16.0.0/255.240.0.0',
    '192.168.0.0/255.255.0.0',
    '169.254.0.0/255.255.0.0',
)


class ServiceGroup(collections.namedtuple('ServiceGroup', (
    'service',
    'dependency_group',
    'mode',
))):
    """A service group.

    :param service: the name of a service
    :param dependency_group: the name of a set of dependencies
                             (the key in dependencies.yaml)
    :param mode: either 'monitor' or 'block'
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
    def rules(self):
        # TODO: actually read these from somewhere
        return (
            iptables.Rule(
                protocol='ip',
                src='0.0.0.0/0.0.0.0',
                dst='0.0.0.0/0.0.0.0',
                target='REJECT',
                matches=(),
            ),
            iptables.Rule(
                protocol='tcp',
                src='0.0.0.0/0.0.0.0',
                dst='169.254.255.254/255.255.255.255',
                target='ACCEPT',
                matches=(
                    ('tcp', (('dport', '20668'),)),
                )
            ),
        )

    def update_rules(self):
        iptables.ensure_chain(self.chain_name, self.rules)


def active_service_groups():
    """Return active service groups."""
    # TODO: actually read these from somewhere
    return {
        ServiceGroup('cool_service', 'main', 'block'): {
            '02:42:a9:fe:00:02',
            'fe:a3:a3:da:2d:51',
            'fe:a3:a3:da:2d:50',
        },
        ServiceGroup('cool_service', 'main', 'monitor'): {
            'fe:a3:a3:da:2d:40',
        },
        ServiceGroup('dumb_service', 'other', 'block'): {
            'fe:a3:a3:da:2d:30',
            'fe:a3:a3:da:2d:31',
        },
    }


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
            ),
        ) + tuple(
            iptables.Rule(
                protocol='ip',
                src='0.0.0.0/0.0.0.0',
                dst=ip_range,
                target='RETURN',
                matches=(),
            )
            for ip_range in PRIVATE_IP_RANGES
        )
    )


def ensure_service_chains():
    """Ensure service chains exist and have the right rules.

    Returns dictionary {[service chain] => [list of mac addresses]}.
    """
    chains = {}
    for service, macs in active_service_groups().items():
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


def general_update():
    """Update iptables to match the current PaaSTA state."""
    ensure_internet_chain()
    service_chains = ensure_service_chains()
    ensure_dispatch_chains(service_chains)
    garbage_collect_old_service_chains(service_chains)
