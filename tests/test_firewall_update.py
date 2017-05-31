# -*- coding: utf-8 -*-
# Copyright 2015-2017 Yelp Inc.
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
from __future__ import absolute_import
from __future__ import unicode_literals

import subprocess

import mock
import pytest
import yaml

from paasta_tools import firewall
from paasta_tools import firewall_update


def test_parse_args_daemon():
    args = firewall_update.parse_args([
        '-d', 'mysoadir',
        '-v',
        'daemon',
        '--synapse-service-dir', 'myservicedir',
        '-u', '123',
    ])
    assert args.mode == 'daemon'
    assert args.synapse_service_dir == 'myservicedir'
    assert args.soa_dir == 'mysoadir'
    assert args.update_secs == 123
    assert args.verbose


def test_parse_args_default_daemon():
    args = firewall_update.parse_args(['daemon'])
    assert args.mode == 'daemon'
    assert args.synapse_service_dir == firewall_update.DEFAULT_SYNAPSE_SERVICE_DIR
    assert args.soa_dir == firewall_update.DEFAULT_SOA_DIR
    assert args.update_secs == firewall_update.DEFAULT_UPDATE_SECS
    assert not args.verbose


def test_parse_args_cron():
    args = firewall_update.parse_args([
        '-d', 'mysoadir',
        '-v',
        'cron',
    ])
    assert args.mode == 'cron'
    assert args.soa_dir == 'mysoadir'
    assert args.verbose


def test_parse_args_default_cron():
    args = firewall_update.parse_args(['cron'])
    assert args.mode == 'cron'
    assert args.soa_dir == firewall_update.DEFAULT_SOA_DIR
    assert not args.verbose


@mock.patch.object(
    firewall_update, 'load_system_paasta_config', autospec=True,
    return_value=mock.Mock(**{
        'get_cluster.return_value': 'mycluster',
    }),
)
@mock.patch.object(
    firewall, 'services_running_here', autospec=True,
    return_value=(
        ('myservice', 'hassecurity', '02:42:a9:fe:00:0a'),
        ('myservice', 'chronoswithsecurity', '02:42:a9:fe:00:0b'),
    ),
)
def test_smartstack_dependencies_of_running_firewalled_services(_, __, tmpdir):
    soa_dir = tmpdir.mkdir('yelpsoa')
    myservice_dir = soa_dir.mkdir('myservice')

    marathon_config = {
        'hassecurity': {
            'dependencies_reference': 'my_ref',
            'security': {
                'outbound_firewall': 'block'
            }
        },
        'nosecurity': {
            'dependencies_reference': 'my_ref',
        }
    }
    myservice_dir.join('marathon-mycluster.yaml').write(yaml.safe_dump(marathon_config))

    chronos_config = {
        'chronoswithsecurity': {
            'dependencies_reference': 'my_ref',
            'security': {
                'outbound_firewall': 'block'
            }
        },
    }
    myservice_dir.join('chronos-mycluster.yaml').write(yaml.safe_dump(chronos_config))

    dependencies_config = {
        'my_ref': [
            {'well-known': 'internet'},
            {'smartstack': 'mydependency.depinstance'},
            {'smartstack': 'another.one'},
        ]
    }
    myservice_dir.join('dependencies.yaml').write(yaml.safe_dump(dependencies_config))

    result = firewall_update.smartstack_dependencies_of_running_firewalled_services(soa_dir=str(soa_dir))
    assert dict(result) == {
        'mydependency.depinstance': {('myservice', 'hassecurity'), ('myservice', 'chronoswithsecurity')},
        'another.one': {('myservice', 'hassecurity'), ('myservice', 'chronoswithsecurity')},
    }


@mock.patch.object(firewall_update, 'smartstack_dependencies_of_running_firewalled_services', autospec=True)
@mock.patch.object(firewall_update, 'process_inotify_event', side_effect=StopIteration, autospec=True)
def test_run_daemon(process_inotify_mock, smartstack_deps_mock, mock_daemon_args):
    class kill_after_too_long(object):
        def __init__(self):
            self.count = 0

        def __call__(self, *args, **kwargs):
            self.count += 1
            assert self.count <= 5, 'Took too long to detect file change'
            return {}

    smartstack_deps_mock.side_effect = kill_after_too_long()
    subprocess.Popen(['bash', '-c', 'sleep 2; echo > %s/mydep.depinstance.json' % mock_daemon_args.synapse_service_dir])
    with pytest.raises(StopIteration):
        firewall_update.run_daemon(mock_daemon_args)
    assert smartstack_deps_mock.call_count > 0
    assert process_inotify_mock.call_args[0][0][3] == b'mydep.depinstance.json'
    assert process_inotify_mock.call_args[0][1] == {}


def test_run_cron(mock_cron_args):
    with mock.patch.object(firewall, 'general_update', autospec=True) as m:
        firewall_update.run_cron(mock_cron_args)
    assert m.called is True


@mock.patch.object(firewall_update, 'log', autospec=True)
def test_process_inotify_event(log_mock):
    # TODO: test something more meaningful than the log function once we have actual iptables
    services_by_dependencies = {
        'mydep.depinstance': {('myservice', 'myinstance'), ('anotherservice', 'instance')}
    }
    firewall_update.process_inotify_event((None, None, None, 'mydep.depinstance.json'), services_by_dependencies)
    assert log_mock.debug.call_count == 2
    log_mock.debug.assert_any_call('Update ', ('myservice', 'myinstance'))
    log_mock.debug.assert_any_call('Update ', ('anotherservice', 'instance'))

    # Verify that tmp writes do not apply
    log_mock.reset_mock()
    firewall_update.process_inotify_event((None, None, None, 'mydep.depinstance.tmp'), services_by_dependencies)
    assert log_mock.debug.call_count == 0


@pytest.fixture
def mock_daemon_args(tmpdir):
    return firewall_update.parse_args([
        '-d', str(tmpdir.mkdir('yelpsoa')),
        'daemon',
        '--synapse-service-dir', str(tmpdir.mkdir('synapse')),
    ])


@pytest.fixture
def mock_cron_args(tmpdir):
    return firewall_update.parse_args([
        '-d', str(tmpdir.mkdir('yelpsoa')),
        'cron',
    ])
