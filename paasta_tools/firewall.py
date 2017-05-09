from __future__ import absolute_import
from __future__ import unicode_literals

import collections
import hashlib

from paasta_tools import iptables


PRIVATE_IP_RANGES = (
    '127.0.0.0/8',
    '10.0.0.0/8',
    '172.16.0.0/12',
    '192.168.0.0/16',
    '169.254.0.0/16',
)


class ServiceGroup(collections.namedtuple('ServiceGroup', (
    'service',
    'dependency_group',
    'mode',
))):
    """A service group.

    :param service: the name of a service
    :param dependency_group: the name of a dependency group
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
        chain += '.' + hashlib.sha256(str(tuple(self))).hexdigest()[:10]
        assert len(chain) <= 28, len(chain)
        return chain

    @property
    def rules(self):
        # TODO: actually read these from somewhere
        return (
            '-j REJECT --reject-with icmp-port-unreachable',
            '-d 169.254.255.254/32 -p tcp -m tcp --dport 20666 -j ACCEPT',
        )

    def update_rules(self):
        iptables.ensure_chain(self.chain_name, self.rules)


def active_service_groups():
    """Return active service groups."""
    # TODO: actually read these from somewhere
    return {
        ServiceGroup('cool-service', 'main', 'block'): {
            '02:42:a9:fe:00:02',
            'fe:a3:a3:da:2d:51',
            'fe:a3:a3:da:2d:50',
        },
        ServiceGroup('cool-service', 'main', 'monitor'): {
            'fe:a3:a3:da:2d:40',
        },
        ServiceGroup('dumb-service', 'other', 'block'): {
            'fe:a3:a3:da:2d:30',
            'fe:a3:a3:da:2d:31',
        },
    }


def general_update():
    """Update iptables to match the current PaaSTA state."""
    # Create the "internet" special chain.
    iptables.ensure_chain(
        'PAASTA-INTERNET',
        (
            '-j ACCEPT',
        ) + tuple(
            '-d {} -j RETURN'.format(ip_range)
            for ip_range in PRIVATE_IP_RANGES
        )
    )

    # Create/update service group chains.
    paasta_rules = []
    active_chains = set()
    for service, macs in active_service_groups().items():
        service.update_rules()
        active_chains.add(service.chain_name)
        for mac in macs:
            paasta_rules.append('-m mac --mac-source {} -j {}'.format(
                mac.upper(), service.chain_name,
            ))

    # Create/update the PAASTA dispatch chain.
    iptables.ensure_chain('PAASTA', paasta_rules)
    iptables.ensure_rule('INPUT', '-j PAASTA')
    iptables.ensure_rule('FORWARD', '-j PAASTA')

    # Garbage collect any no-longer-needed service group chains.
    paasta_chains = {
        chain
        for chain in iptables.all_chains()
        if chain.startswith('PAASTA.')
    }
    for chain in paasta_chains - active_chains:
        iptables.delete_chain(chain)


# TODO: remove this
general_update()
