# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import iptc
import mock
import pytest

from paasta_tools import iptables


EMPTY_RULE = iptables.Rule(
    protocol='ip',
    src='0.0.0.0/0.0.0.0',
    dst='0.0.0.0/0.0.0.0',
    target=None,
    matches=(),
)


@pytest.yield_fixture
def mock_Table():
    with mock.patch.object(
        iptc, 'Table', autospec=True,
    ) as m:
        m.return_value.autocommit = True
        yield m


@pytest.yield_fixture
def mock_Chain():
    with mock.patch.object(
        iptc, 'Chain', autospec=True,
    ) as m:
        yield m


def test_rule_from_iptc_simple():
    rule = iptc.Rule()
    rule.create_target('DROP')
    rule.src = '169.229.226.0/255.255.255.0'

    assert iptables.Rule.from_iptc(rule) == EMPTY_RULE._replace(
        src='169.229.226.0/255.255.255.0',
        target='DROP',
    )


def test_rule_from_iptc_mac_match():
    rule = iptc.Rule()
    rule.create_target('DROP')
    rule.create_match('mac')
    rule.matches[0].mac_source = '20:C9:D0:2B:6F:F3'

    assert iptables.Rule.from_iptc(rule) == EMPTY_RULE._replace(
        target='DROP',
        matches=(
            ('mac', (
                ('mac_source', '20:C9:D0:2B:6F:F3'),
            )),
        ),
    )


def test_rule_tcp_to_iptc():
    rule = EMPTY_RULE._replace(
        protocol='tcp',
        target='ACCEPT',
        matches=(
            ('tcp', (
                ('dport', '443'),
            )),
        ),
    ).to_iptc()
    assert rule.protocol == 'tcp'
    assert rule.target.name == 'ACCEPT'
    assert len(rule.matches) == 1
    assert rule.matches[0].name == 'tcp'
    assert rule.matches[0].parameters['dport'] == '443'


def test_mac_src_to_iptc():
    rule = EMPTY_RULE._replace(
        target='ACCEPT',
        matches=(
            ('mac', (
                ('mac_source', '20:C9:D0:2B:6F:F3'),
            )),
        ),
    ).to_iptc()
    assert rule.protocol == 'ip'
    assert rule.target.name == 'ACCEPT'
    assert len(rule.matches) == 1
    assert rule.matches[0].name == 'mac'
    assert rule.matches[0].parameters['mac_source'] == '20:C9:D0:2B:6F:F3'


def test_iptables_txn_normal():
    table = mock.Mock(autocommit=True)
    with iptables.iptables_txn(table):
        assert table.autocommit is False
        assert table.commit.called is False
        assert table.refresh.called is False
    assert table.commit.called is True
    assert table.refresh.called is True
    assert table.autocommit is True


def test_iptables_txn_with_exception():
    table = mock.Mock(autocommit=True)
    with pytest.raises(ValueError):
        with iptables.iptables_txn(table):
            raise ValueError('just testing lol')
    assert table.commit.called is False
    assert table.refresh.called is True
    assert table.autocommit is True


def test_all_chains(mock_Table):
    chain1 = mock.Mock()
    chain1.name = 'INPUT'
    chain2 = mock.Mock()
    chain2.name = 'OUTPUT'
    mock_Table.return_value = mock.Mock(chains=[
        chain1, chain2,
    ])
    assert iptables.all_chains() == {'INPUT', 'OUTPUT'}


def test_ensure_chain():
    with mock.patch.object(
        iptables, 'list_chain', autospec=True, return_value={
            EMPTY_RULE._replace(target='DROP'),
            EMPTY_RULE._replace(target='ACCEPT', src='1.0.0.0/255.255.255.0'),
        },
    ), mock.patch.object(
        iptables, 'insert_rule', autospec=True,
    ) as mock_insert_rule, mock.patch.object(
        iptables, 'delete_rules', autospec=True,
    ) as mock_delete_rules:
        iptables.ensure_chain('PAASTA.service', (
            EMPTY_RULE._replace(target='DROP'),
            EMPTY_RULE._replace(target='ACCEPT', src='2.0.0.0/255.255.255.0'),
        ))

    # It should add the missing rule
    assert mock_insert_rule.mock_calls == [
        mock.call(
            'PAASTA.service',
            EMPTY_RULE._replace(target='ACCEPT', src='2.0.0.0/255.255.255.0'),
        ),
    ]

    # It should delete the extra rule
    assert mock_delete_rules.mock_calls == [
        mock.call(
            'PAASTA.service',
            {EMPTY_RULE._replace(target='ACCEPT', src='1.0.0.0/255.255.255.0')},
        ),
    ]


def test_ensure_chain_creates_chain_if_doesnt_exist():
    with mock.patch.object(
        iptables, 'list_chain',
        side_effect=iptables.ChainDoesNotExist('PAASTA.service')
    ), mock.patch.object(
        iptables, 'create_chain', autospec=True,
    ) as mock_create_chain:
        iptables.ensure_chain('PAASTA.service', ())

    assert mock_create_chain.mock_calls == [
        mock.call('PAASTA.service'),
    ]


def test_ensure_rule_does_not_exist():
    with mock.patch.object(
        iptables, 'list_chain', return_value=(
            EMPTY_RULE._replace(target='ACCEPT'),
            EMPTY_RULE._replace(src='10.0.0.0/255.255.255.0'),
        ),
    ), mock.patch.object(
        iptables, 'insert_rule', autospec=True,
    ) as mock_insert_rule:
        iptables.ensure_rule(
            'PAASTA.service', EMPTY_RULE._replace(target='DROP'),
        )

    assert mock_insert_rule.mock_calls == [
        mock.call('PAASTA.service', EMPTY_RULE._replace(target='DROP')),
    ]


def test_ensure_rule_already_exists():
    with mock.patch.object(
        iptables, 'list_chain', return_value=(
            EMPTY_RULE._replace(target='DROP'),
            EMPTY_RULE._replace(src='10.0.0.0/255.255.255.0'),
        ),
    ), mock.patch.object(
        iptables, 'insert_rule', autospec=True,
    ) as mock_insert_rule:
        iptables.ensure_rule(
            'PAASTA.service', EMPTY_RULE._replace(target='DROP'),
        )

    assert mock_insert_rule.called is False


def test_insert_rule(mock_Table, mock_Chain):
    iptables.insert_rule(
        'PAASTA.service', EMPTY_RULE._replace(target='DROP'),
    )

    call, = mock_Chain('filter', 'PAASTA.service').insert_rule.call_args_list
    args, kwargs = call
    rule, = args
    assert iptables.Rule.from_iptc(rule) == EMPTY_RULE._replace(target='DROP')


def test_delete_rules(mock_Table, mock_Chain):
    mock_Chain.return_value.rules = (
        EMPTY_RULE._replace(target='DROP').to_iptc(),
        EMPTY_RULE._replace(target='ACCEPT').to_iptc(),
        EMPTY_RULE._replace(target='REJECT').to_iptc(),
    )
    iptables.delete_rules('PAASTA.service', (
        EMPTY_RULE._replace(target='ACCEPT'),
        EMPTY_RULE._replace(target='REJECT'),
    ))
    assert mock_Chain('filter', 'PAASTA.service').delete_rule.mock_calls == [
        mock.call(mock_Chain.return_value.rules[1]),
        mock.call(mock_Chain.return_value.rules[2]),
    ]


def test_create_chain(mock_Table):
    iptables.create_chain('PAASTA.service')
    mock_Table('filter').create_chain.assert_called_once_with('PAASTA.service')


def test_delete_chain(mock_Table, mock_Chain):
    iptables.delete_chain('PAASTA.service')
    chain = mock_Chain('filter', 'PAASTA.service')
    assert chain.flush.called is True
    assert chain.delete.called is True


def test_list_chain_simple(mock_Table, mock_Chain):
    chain = mock_Chain('PAASTA.internet', mock_Table.return_value)
    rule = iptc.Rule()
    rule.create_target('DROP')
    chain.rules = [rule]
    mock_Table.return_value.chains = [chain]
    assert iptables.list_chain('PAASTA.internet') == {
        EMPTY_RULE._replace(target='DROP'),
    }


def test_list_chain_does_not_exist(mock_Table, mock_Chain):
    mock_Table.return_value.chains = []
    with pytest.raises(iptables.ChainDoesNotExist):
        iptables.list_chain('PAASTA.internet')
