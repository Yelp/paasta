# Copyright 2015-2016 Yelp Inc.
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
import contextlib

import mock
from pytest import raises

from paasta_tools import long_running_service_tools
from paasta_tools.utils import InvalidInstanceConfig


def test_get_healthcheck_cmd_happy():
    fake_conf = long_running_service_tools.LongRunningServiceConfig(
        service='fake_name',
        cluster='fake_cluster',
        instance='fake_instance',
        config_dict={'healthcheck_cmd': 'test_cmd'},
        branch_dict={},
    )
    actual = fake_conf.get_healthcheck_cmd()
    assert actual == 'test_cmd'


def test_get_healthcheck_cmd_raises_when_unset():
    fake_conf = long_running_service_tools.LongRunningServiceConfig(
        service='fake_name',
        cluster='fake_cluster',
        instance='fake_instance',
        config_dict={},
        branch_dict={},
    )
    with raises(InvalidInstanceConfig) as exc:
        fake_conf.get_healthcheck_cmd()
    assert "healthcheck mode 'cmd' requires a healthcheck_cmd to run" in str(exc.value)


def test_get_healthcheck_for_instance_http():
    fake_service = 'fake_service'
    fake_namespace = 'fake_namespace'
    fake_hostname = 'fake_hostname'
    fake_random_port = 666

    fake_path = '/fake_path'
    fake_service_config = long_running_service_tools.LongRunningServiceConfig(
        service=fake_service,
        cluster='fake_cluster',
        instance=fake_namespace,
        config_dict={},
        branch_dict={},
    )
    fake_service_namespace_config = long_running_service_tools.ServiceNamespaceConfig({
        'mode': 'http',
        'healthcheck_uri': fake_path,
    })
    with contextlib.nested(
        mock.patch('paasta_tools.long_running_service_tools.load_service_namespace_config',
                   autospec=True,
                   return_value=fake_service_namespace_config),
        mock.patch('socket.getfqdn', autospec=True, return_value=fake_hostname),
    ) as (
        load_service_namespace_config_patch,
        hostname_patch
    ):
        expected = ('http', 'http://%s:%d%s' % (fake_hostname, fake_random_port, fake_path))
        actual = long_running_service_tools.get_healthcheck_for_instance(
            fake_service, fake_namespace, fake_service_config, fake_random_port)
        assert expected == actual


def test_get_healthcheck_for_instance_tcp():
    fake_service = 'fake_service'
    fake_namespace = 'fake_namespace'
    fake_hostname = 'fake_hostname'
    fake_random_port = 666

    fake_service_config = long_running_service_tools.LongRunningServiceConfig(
        service=fake_service,
        cluster='fake_cluster',
        instance=fake_namespace,
        config_dict={},
        branch_dict={},
    )
    fake_service_namespace_config = long_running_service_tools.ServiceNamespaceConfig({
        'mode': 'tcp',
    })
    with contextlib.nested(
        mock.patch('paasta_tools.long_running_service_tools.load_service_namespace_config',
                   autospec=True,
                   return_value=fake_service_namespace_config),
        mock.patch('socket.getfqdn', autospec=True, return_value=fake_hostname),
    ) as (
        load_service_namespace_config_patch,
        hostname_patch
    ):
        expected = ('tcp', 'tcp://%s:%d' % (fake_hostname, fake_random_port))
        actual = long_running_service_tools.get_healthcheck_for_instance(
            fake_service, fake_namespace, fake_service_config, fake_random_port)
        assert expected == actual


def test_get_healthcheck_for_instance_cmd():
    fake_service = 'fake_service'
    fake_namespace = 'fake_namespace'
    fake_hostname = 'fake_hostname'
    fake_random_port = 666
    fake_cmd = '/bin/fake_command'
    fake_service_config = long_running_service_tools.LongRunningServiceConfig(
        service=fake_service,
        cluster='fake_cluster',
        instance=fake_namespace,
        config_dict={
            'healthcheck_mode': 'cmd',
            'healthcheck_cmd': fake_cmd
        },
        branch_dict={},
    )
    fake_service_namespace_config = long_running_service_tools.ServiceNamespaceConfig({})
    with contextlib.nested(
        mock.patch('paasta_tools.long_running_service_tools.load_service_namespace_config',
                   autospec=True,
                   return_value=fake_service_namespace_config),
        mock.patch('socket.getfqdn', autospec=True, return_value=fake_hostname),
    ) as (
        load_service_namespace_config_patch,
        hostname_patch
    ):
        expected = ('cmd', fake_cmd)
        actual = long_running_service_tools.get_healthcheck_for_instance(
            fake_service, fake_namespace, fake_service_config, fake_random_port)
        assert expected == actual


def test_get_healthcheck_for_instance_other():
    fake_service = 'fake_service'
    fake_namespace = 'fake_namespace'
    fake_hostname = 'fake_hostname'
    fake_random_port = 666
    fake_service_config = long_running_service_tools.LongRunningServiceConfig(
        service=fake_service,
        cluster='fake_cluster',
        instance=fake_namespace,
        config_dict={
            'healthcheck_mode': None,
        },
        branch_dict={},
    )
    fake_service_namespace_config = long_running_service_tools.ServiceNamespaceConfig({})
    with contextlib.nested(
        mock.patch('paasta_tools.long_running_service_tools.load_service_namespace_config',
                   autospec=True,
                   return_value=fake_service_namespace_config),
        mock.patch('socket.getfqdn', autospec=True, return_value=fake_hostname),
    ) as (
        load_service_namespace_config_patch,
        hostname_patch
    ):
        expected = (None, None)
        actual = long_running_service_tools.get_healthcheck_for_instance(
            fake_service, fake_namespace, fake_service_config, fake_random_port)
        assert expected == actual


def test_get_healthcheck_for_instance_custom_soadir():
    fake_service = 'fake_service'
    fake_namespace = 'fake_namespace'
    fake_hostname = 'fake_hostname'
    fake_random_port = 666
    fake_soadir = '/fake/soadir'
    fake_service_config = long_running_service_tools.LongRunningServiceConfig(
        service=fake_service,
        cluster='fake_cluster',
        instance=fake_namespace,
        config_dict={
            'healthcheck_mode': None,
        },
        branch_dict={},
    )
    fake_service_namespace_config = long_running_service_tools.ServiceNamespaceConfig({})
    with contextlib.nested(
        mock.patch('paasta_tools.long_running_service_tools.load_service_namespace_config',
                   autospec=True,
                   return_value=fake_service_namespace_config),
        mock.patch('socket.getfqdn', autospec=True, return_value=fake_hostname),
    ) as (
        load_service_namespace_config_patch,
        hostname_patch
    ):
        expected = (None, None)
        actual = long_running_service_tools.get_healthcheck_for_instance(
            fake_service, fake_namespace, fake_service_config, fake_random_port, soa_dir=fake_soadir)
        assert expected == actual
        load_service_namespace_config_patch.assert_called_once_with(fake_service, fake_namespace, fake_soadir)
