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
from __future__ import absolute_import
from __future__ import unicode_literals

import os

import mock
import requests

from paasta_tools import smartstack_tools
from paasta_tools.smartstack_tools import backend_is_up
from paasta_tools.smartstack_tools import get_registered_marathon_tasks
from paasta_tools.smartstack_tools import get_replication_for_services
from paasta_tools.smartstack_tools import ip_port_hostname_from_svname
from paasta_tools.smartstack_tools import match_backends_and_tasks
from paasta_tools.utils import DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT
from paasta_tools.utils import SystemPaastaConfig


def test_load_smartstack_info_for_service():
    with mock.patch(
        'paasta_tools.smartstack_tools.marathon_tools.load_service_namespace_config',
        autospec=True,
    ), mock.patch(
        'paasta_tools.smartstack_tools.get_smartstack_replication_for_attribute',
        autospec=True,
    ):
        # just a smoke test for now.
        smartstack_tools.load_smartstack_info_for_service(
            service='service',
            namespace='namespace',
            soa_dir='fake',
            blacklist=[],
            system_paasta_config=SystemPaastaConfig({}, '/fake/config'),
        )


def test_get_smartstack_replication_for_attribute():
    fake_namespace = 'fake_main'
    fake_service = 'fake_service'
    mock_filtered_slaves = [
        {
            'hostname': 'hostone',
            'attributes': {
                'fake_attribute': 'foo',
            },
        },
        {
            'hostname': 'hostone',
            'attributes': {
                'fake_attribute': 'bar',
            },
        },
    ]

    fake_system_paasta_config = SystemPaastaConfig({}, '/fake/config')
    with mock.patch(
        'paasta_tools.mesos_tools.get_all_slaves_for_blacklist_whitelist',
        return_value=mock_filtered_slaves, autospec=True,
    ) as mock_get_all_slaves_for_blacklist_whitelist, mock.patch(
        'paasta_tools.smartstack_tools.get_replication_for_services',
        return_value={}, autospec=True,
    ) as mock_get_replication_for_services:
        expected = {
            'foo': {},
            'bar': {},
        }
        actual = smartstack_tools.get_smartstack_replication_for_attribute(
            attribute='fake_attribute',
            service=fake_service,
            namespace=fake_namespace,
            blacklist=[],
            system_paasta_config=fake_system_paasta_config,
        )
        mock_get_all_slaves_for_blacklist_whitelist.assert_called_once_with(
            blacklist=[],
            whitelist=[],
        )
        assert actual == expected
        assert mock_get_replication_for_services.call_count == 2

        mock_get_replication_for_services.assert_any_call(
            synapse_host='hostone',
            synapse_port=fake_system_paasta_config.get_synapse_port(),
            synapse_haproxy_url_format=fake_system_paasta_config.get_synapse_haproxy_url_format(),
            services=['fake_service.fake_main'],
        )


def test_get_replication_for_service():
    testdir = os.path.dirname(os.path.realpath(__file__))
    testdata = os.path.join(testdir, 'haproxy_snapshot.txt')
    with open(testdata, 'r') as fd:
        mock_haproxy_data = fd.read()

    mock_response = mock.Mock()
    mock_response.text = mock_haproxy_data
    mock_get = mock.Mock(return_value=(mock_response))

    with mock.patch.object(requests.Session, 'get', mock_get):
        replication_result = get_replication_for_services(
            'fake_host',
            6666,
            DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
            ['service1', 'service2', 'service3', 'service4'],
        )
        expected = {
            'service1': 18,
            'service2': 19,
            'service3': 0,
            'service4': 3,
        }
        assert expected == replication_result


def test_get_registered_marathon_tasks():
    backends = [
        {"pxname": "servicename.main", "svname": "10.50.2.4:31000_box4", "status": "UP"},
        {"pxname": "servicename.main", "svname": "10.50.2.5:31001_box5", "status": "UP"},
        {"pxname": "servicename.main", "svname": "10.50.2.6:31001_box6", "status": "UP"},
        {"pxname": "servicename.main", "svname": "10.50.2.6:31002_box7", "status": "UP"},
        {"pxname": "servicename.main", "svname": "10.50.2.8:31000_box8", "status": "UP"},
    ]

    hostnames = {
        'box4': '10.50.2.4',
        'box5': '10.50.2.5',
        'box6': '10.50.2.6',
        'box7': '10.50.2.7',
        'box8': '10.50.2.8',
    }

    good_task1 = mock.Mock(host='box4', ports=[31000])
    good_task2 = mock.Mock(host='box5', ports=[31001])
    bad_task = mock.Mock(host='box7', ports=[31000])

    marathon_tasks = [
        good_task1,
        good_task2,
        bad_task,
    ]

    with mock.patch(
        'paasta_tools.smartstack_tools.get_multiple_backends',
        return_value=backends,
        autospec=True,
    ) as mock_get_multiple_backends:
        with mock.patch(
            'paasta_tools.smartstack_tools.socket.gethostbyname',
            side_effect=lambda x: hostnames[x],
            autospec=True,
        ):
            actual = get_registered_marathon_tasks(
                'fake_host',
                6666,
                DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
                'servicename.main',
                marathon_tasks,
            )

            expected = [good_task1, good_task2]
            assert actual == expected

            mock_get_multiple_backends.assert_called_once_with(
                ['servicename.main'],
                synapse_host='fake_host',
                synapse_port=6666,
                synapse_haproxy_url_format=DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT,
            )


def test_backend_is_up():
    assert True is backend_is_up({"status": "UP"})
    assert True is backend_is_up({"status": "UP 1/2"})
    assert False is backend_is_up({"status": "DOWN"})
    assert False is backend_is_up({"status": "DOWN 1/2"})
    assert False is backend_is_up({"status": "MAINT"})


def test_ip_port_hostname_from_svname_new_format():
    assert ("10.40.10.155", 31219, "myhost") == ip_port_hostname_from_svname("myhost_10.40.10.155:31219")


def test_ip_port_hostname_from_svname_old_format():
    assert ("10.85.5.101", 3744, "myhost") == ip_port_hostname_from_svname("10.85.5.101:3744_myhost")


def test_match_backends_and_tasks():
    backends = [
        {"pxname": "servicename.main", "svname": "10.50.2.4:31000_box4", "status": "UP"},
        {"pxname": "servicename.main", "svname": "10.50.2.5:31001_box5", "status": "UP"},
        {"pxname": "servicename.main", "svname": "10.50.2.6:31001_box6", "status": "UP"},
        {"pxname": "servicename.main", "svname": "10.50.2.6:31002_box7", "status": "UP"},
        {"pxname": "servicename.main", "svname": "10.50.2.8:31000_box8", "status": "UP"},
    ]

    hostnames = {
        'box4': '10.50.2.4',
        'box5': '10.50.2.5',
        'box6': '10.50.2.6',
        'box7': '10.50.2.7',
        'box8': '10.50.2.8',
    }

    good_task1 = mock.Mock(host='box4', ports=[31000])
    good_task2 = mock.Mock(host='box5', ports=[31001])
    bad_task = mock.Mock(host='box7', ports=[31000])
    tasks = [good_task1, good_task2, bad_task]

    with mock.patch(
        'paasta_tools.smartstack_tools.socket.gethostbyname',
        side_effect=lambda x: hostnames[x],
        autospec=True,
    ):
        expected = [
            (backends[0], good_task1),
            (backends[1], good_task2),
            (None, bad_task),
            (backends[2], None),
            (backends[3], None),
            (backends[4], None),
        ]
        actual = match_backends_and_tasks(backends, tasks)

        def keyfunc(t):
            return tuple(sorted((t[0] or {}).items())), t[1]
        assert sorted(actual, key=keyfunc) == sorted(expected, key=keyfunc)
