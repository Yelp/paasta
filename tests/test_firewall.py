# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from paasta_tools import firewall
from paasta_tools import iptables


EMPTY_RULE = iptables.Rule(
    protocol='ip',
    src='0.0.0.0/0.0.0.0',
    dst='0.0.0.0/0.0.0.0',
    target=None,
    matches=(),
)


@pytest.fixture
def service_group():
    return firewall.ServiceGroup(
        service='my_cool_service',
        dependency_group='web',
        mode='block',
    )


def test_service_group_chain_name(service_group):
    """The chain name must be stable, unique, and short."""
    assert service_group.chain_name == 'PAASTA.my_cool_se.7693eadc05'
    assert len(service_group.chain_name) <= 28


def test_service_group_rules(service_group):
    # TODO: this is a useless test right now
    assert len(service_group.rules) == 2


def test_service_group_update_rules(service_group):
    with mock.patch.object(iptables, 'ensure_chain', autospec=True) as m:
        service_group.update_rules()
    m.assert_called_once_with(
        service_group.chain_name,
        service_group.rules,
    )


def test_active_service_groups():
    # TODO: this is a useless test right now
    assert len(firewall.active_service_groups()) == 3


def test_ensure_internet_chain():
    with mock.patch.object(iptables, 'ensure_chain', autospec=True) as m:
        firewall.ensure_internet_chain()
    call, = m.call_args_list
    args, _ = call
    assert args[0] == 'PAASTA-INTERNET'
    assert args[1] == (
        EMPTY_RULE._replace(target='ACCEPT'),
        EMPTY_RULE._replace(dst='127.0.0.0/255.0.0.0', target='RETURN'),
        EMPTY_RULE._replace(dst='10.0.0.0/255.0.0.0', target='RETURN'),
        EMPTY_RULE._replace(dst='172.16.0.0/255.240.0.0', target='RETURN'),
        EMPTY_RULE._replace(dst='192.168.0.0/255.255.0.0', target='RETURN'),
        EMPTY_RULE._replace(dst='169.254.0.0/255.255.0.0', target='RETURN'),
    )


@pytest.yield_fixture
def mock_active_service_groups():
    groups = {
        firewall.ServiceGroup('cool_service', 'main', 'block'): {
            '02:42:a9:fe:00:02',
            'fe:a3:a3:da:2d:51',
            'fe:a3:a3:da:2d:50',
        },
        firewall.ServiceGroup('cool_service', 'main', 'monitor'): {
            'fe:a3:a3:da:2d:40',
        },
        firewall.ServiceGroup('dumb_service', 'other', 'block'): {
            'fe:a3:a3:da:2d:30',
            'fe:a3:a3:da:2d:31',
        },
    }
    with mock.patch.object(
            firewall,
            'active_service_groups',
            return_value=groups,
    ):
        yield


def test_ensure_service_chains(mock_active_service_groups):
    with mock.patch.object(iptables, 'ensure_chain', autospec=True) as m:
        assert firewall.ensure_service_chains() == {
            'PAASTA.cool_servi.130d5afc9f': {
                '02:42:a9:fe:00:02',
                'fe:a3:a3:da:2d:51',
                'fe:a3:a3:da:2d:50',
            },
            'PAASTA.cool_servi.0d2d779529': {
                'fe:a3:a3:da:2d:40',
            },
            'PAASTA.dumb_servi.06f185ab16': {
                'fe:a3:a3:da:2d:30',
                'fe:a3:a3:da:2d:31',
            },
        }
    assert len(m.mock_calls) == 3
    assert mock.call('PAASTA.cool_servi.130d5afc9f', mock.ANY) in m.mock_calls
    assert mock.call('PAASTA.cool_servi.0d2d779529', mock.ANY) in m.mock_calls
    assert mock.call('PAASTA.dumb_servi.06f185ab16', mock.ANY) in m.mock_calls


def test_ensure_dispatch_chains():
    with mock.patch.object(
        iptables, 'ensure_rule', autospec=True,
    ) as mock_ensure_rule, mock.patch.object(
        iptables, 'ensure_chain', autospec=True,
    ) as mock_ensure_chain:
        firewall.ensure_dispatch_chains({
            'chain1': {'mac1', 'mac2'},
            'chain2': {'mac3'},
        })

    assert mock_ensure_chain.mock_calls == [mock.call(
        'PAASTA', {
            EMPTY_RULE._replace(
                target='chain1', matches=(('mac', (('mac_source', 'MAC1'),)),),
            ),
            EMPTY_RULE._replace(
                target='chain1', matches=(('mac', (('mac_source', 'MAC2'),)),),
            ),
            EMPTY_RULE._replace(
                target='chain2', matches=(('mac', (('mac_source', 'MAC3'),)),),
            ),
        },
    )]

    assert mock_ensure_rule.mock_calls == [
        mock.call('INPUT', EMPTY_RULE._replace(target='PAASTA')),
        mock.call('FORWARD', EMPTY_RULE._replace(target='PAASTA')),
    ]


def test_garbage_collect_old_service_chains():
    with mock.patch.object(
        iptables, 'delete_chain', autospec=True,
    ) as mock_delete_chain, mock.patch.object(
        iptables, 'all_chains', autospec=True, return_value={
            'INPUT',
            'OUTPUT',
            'FORWARD',
            'DOCKER',
            'PAASTA',
            'PAASTA-INTERNET',
            'PAASTA.chain1',
            'PAASTA.chain3',
        }
    ):
        firewall.garbage_collect_old_service_chains({
            'PAASTA.chain1': {'mac1', 'mac2'},
            'PAASTA.chain2': {'mac3'},
        })

    assert mock_delete_chain.mock_calls == [
        mock.call('PAASTA.chain3'),
    ]
