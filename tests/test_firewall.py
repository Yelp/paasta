# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest

from paasta_tools import firewall
from paasta_tools import iptables
from paasta_tools.utils import DEFAULT_SOA_DIR


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
        instance='web',
        soa_dir=DEFAULT_SOA_DIR,
    )


@pytest.yield_fixture
def mock_get_running_mesos_docker_containers():
    with mock.patch.object(
        firewall, 'get_running_mesos_docker_containers', autospec=True,
        return_value=[
            {
                'HostConfig': {'NetworkMode': 'bridge'},
                'Labels': {
                    'paasta_service': 'myservice',
                    'paasta_instance': 'hassecurity',
                },
                'NetworkSettings': {
                    'Networks': {
                        'bridge': {'MacAddress': '02:42:a9:fe:00:0a'},
                    },
                },
            },
            {
                'HostConfig': {'NetworkMode': 'bridge'},
                'Labels': {
                    'paasta_service': 'myservice',
                    'paasta_instance': 'chronoswithsecurity',
                },
                'NetworkSettings': {
                    'Networks': {
                        'bridge': {'MacAddress': '02:42:a9:fe:00:0b'},
                    },
                },
            },
            # host networking
            {
                'HostConfig': {'NetworkMode': 'host'},
                'Labels': {
                    'paasta_service': 'myservice',
                    'paasta_instance': 'batch',
                },
            },
            # no labels
            {
                'HostConfig': {'NetworkMode': 'bridge'},
                'Labels': {},
            },
        ],
    ):
        yield


@pytest.mark.usefixtures('mock_get_running_mesos_docker_containers')
def test_services_running_here():
    assert tuple(firewall.services_running_here()) == (
        ('myservice', 'hassecurity', '02:42:a9:fe:00:0a'),
        ('myservice', 'chronoswithsecurity', '02:42:a9:fe:00:0b'),
    )


@pytest.yield_fixture
def mock_services_running_here():
    with mock.patch.object(
        firewall, 'services_running_here', autospec=True,
        side_effect=lambda: iter((
            ('example_happyhour', 'main', '02:42:a9:fe:00:00'),
            ('example_happyhour', 'main', '02:42:a9:fe:00:01'),
            ('example_happyhour', 'batch', '02:42:a9:fe:00:02'),
            ('my_cool_service', 'web', '02:42:a9:fe:00:03'),
            ('my_cool_service', 'web', '02:42:a9:fe:00:04'),
        )),
    ):
        yield


def test_service_group_chain_name(service_group):
    """The chain name must be stable, unique, and short."""
    assert service_group.chain_name == 'PAASTA.my_cool_se.da964afae9'
    assert len(service_group.chain_name) <= 28


@pytest.yield_fixture
def mock_service_config():
    with mock.patch.object(
        firewall.ServiceGroup, 'config', new_callable=mock.PropertyMock,
    ) as m, mock.patch.object(
        firewall, 'get_all_namespaces_for_service', autospec=True,
        return_value={'example_happyhour.main': {'proxy_port': '20000'}},
    ):
        m.return_value = mock.Mock()
        m.return_value.get_dependencies.return_value = {
            'well-known': ('internet',),
            'smartstack': (
                'example_happyhour.main',
            ),
        }
        m.return_value.get_outbound_firewall.return_value = 'monitor'
        yield


def test_service_group_rules(mock_service_config, service_group):
    assert service_group.rules == (
        EMPTY_RULE._replace(target='LOG'),
        EMPTY_RULE._replace(target='PAASTA-INTERNET'),
        EMPTY_RULE._replace(
            protocol='tcp',
            target='ACCEPT',
            dst='169.254.255.254/255.255.255.255',
            matches=(
                ('tcp', (('dport', '20000'),)),
            ),
        ),
    )


def test_service_group_update_rules(service_group):
    with mock.patch.object(iptables, 'ensure_chain', autospec=True) as m:
        with mock.patch.object(type(service_group), 'rules', mock.sentinel.RULES):
            service_group.update_rules()
    m.assert_called_once_with(
        service_group.chain_name,
        mock.sentinel.RULES,
    )


def test_active_service_groups(mock_service_config, mock_services_running_here):
    assert firewall.active_service_groups(DEFAULT_SOA_DIR) == {
        firewall.ServiceGroup('example_happyhour', 'main', DEFAULT_SOA_DIR): {
            '02:42:a9:fe:00:00',
            '02:42:a9:fe:00:01',
        },
        firewall.ServiceGroup('example_happyhour', 'batch', DEFAULT_SOA_DIR): {
            '02:42:a9:fe:00:02',
        },
        firewall.ServiceGroup('my_cool_service', 'web', DEFAULT_SOA_DIR): {
            '02:42:a9:fe:00:03',
            '02:42:a9:fe:00:04',
        },
    }


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
        firewall.ServiceGroup('cool_service', 'main', DEFAULT_SOA_DIR): {
            '02:42:a9:fe:00:02',
            'fe:a3:a3:da:2d:51',
            'fe:a3:a3:da:2d:50',
        },
        firewall.ServiceGroup('cool_service', 'main', DEFAULT_SOA_DIR): {
            'fe:a3:a3:da:2d:40',
        },
        firewall.ServiceGroup('dumb_service', 'other', DEFAULT_SOA_DIR): {
            'fe:a3:a3:da:2d:30',
            'fe:a3:a3:da:2d:31',
        },
    }
    with mock.patch.object(
            firewall,
            'active_service_groups',
            autospec=True,
            return_value=groups,
    ):
        yield


def test_ensure_service_chains(mock_active_service_groups, mock_service_config):
    with mock.patch.object(iptables, 'ensure_chain', autospec=True) as m:
        assert firewall.ensure_service_chains(DEFAULT_SOA_DIR) == {
            'PAASTA.cool_servi.771bae24b0': {
                'fe:a3:a3:da:2d:40',
            },
            'PAASTA.dumb_servi.b3e0fd962a': {
                'fe:a3:a3:da:2d:30',
                'fe:a3:a3:da:2d:31',
            },
        }
    assert len(m.mock_calls) == 2
    assert mock.call('PAASTA.cool_servi.771bae24b0', mock.ANY) in m.mock_calls
    assert mock.call('PAASTA.dumb_servi.b3e0fd962a', mock.ANY) in m.mock_calls


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
