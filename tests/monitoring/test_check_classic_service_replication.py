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

import sys

import mock
import pytest
import requests

from paasta_tools.monitoring.check_classic_service_replication import ClassicServiceReplicationCheck
from paasta_tools.monitoring.check_classic_service_replication import do_replication_check
from paasta_tools.monitoring.check_classic_service_replication import extract_replication_info
from paasta_tools.monitoring.check_classic_service_replication import report_event
from paasta_tools.monitoring.config_providers import MonitoringInfo  # noqa
from paasta_tools.utils import DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT
from paasta_tools.utils import SystemPaastaConfig


def test_report_event():
    with mock.patch('pysensu_yelp.send_event', autospec=True) as mock_call:
        report_event(
            {
                'team': 'search_infra',
                'name': 'Fake Event',
                'runbook': 'Fake Runbook',
                'output': 'Foo',
                'status': 'OK',
            },
        )
        mock_call.assert_called_with(
            team='search_infra',
            name='Fake Event',
            runbook='Fake Runbook',
            output='Foo',
            status='OK',
        )

        with pytest.raises(Exception):
            report_event({'foo': 'bar'})


def test_do_replication_check():
    base_module = 'paasta_tools.monitoring'
    check_classic_module = base_module + '.check_classic_service_replication'

    check_method = check_classic_module + '.check_replication'
    read_key_method = check_classic_module + '.read_key'

    mock_keys = [
        'team', 'notification_email', 'runbook', 'tip', 'page',
        'alert_after', 'realert_every',
    ]

    mock_default_data: MonitoringInfo = {key: None for key in mock_keys}
    mock_default_data['team'] = 'test_team'

    mock_specific_data: MonitoringInfo = {
        key: "test_{}".format(key) for key in mock_keys
    }
    mock_specific_data['extra'] = {
        'replication': {
            'key': 'test_key',
            'default': 'test_default',
            'map': 'test_map',
        },
    }

    with mock.patch(
        check_method, return_value=(-1, 'bar'), autospec=True,
    ), mock.patch(
        read_key_method, return_value=-2, autospec=True,
    ):
        expected = {
            'name': 'replication_test_service',
            'status': -1,
            'output': 'bar',
            'team': 'test_team',
            'notification_email': None,
            'runbook': 'no runbook',
            'tip': 'no tip',
            'page': False,
            'check_every': '1m',
            'alert_after': '0s',
            'realert_every': -1,
        }
        results = do_replication_check(
            'test_service', mock_default_data,
            3,
        )
        assert expected == results

        expected = {
            'name': 'replication_test_service',
            'status': -1,
            'output': 'bar',
            'team': 'test_team',
            'notification_email': 'test_notification_email',
            'runbook': 'test_runbook',
            'tip': 'test_tip',
            'page': 'test_page',
            'check_every': '1m',
            'alert_after': 'test_alert_after',
            'realert_every': 'test_realert_every',
        }
        results = do_replication_check(
            'test_service', mock_specific_data,
            3,
        )
        assert expected == results


def test_extract_replication_info_valid_data():
    base_module = 'paasta_tools.monitoring'
    check_classic_module = base_module + '.check_classic_service_replication'
    extract_method = check_classic_module + '.extract_monitoring_info'

    mock_valid_data = {
        'team': 'test_team',
        'service_type': 'classic',
    }
    with mock.patch(extract_method, return_value=mock_valid_data, autospec=True):
        expected = (True, mock_valid_data)
        result = extract_replication_info({})
        assert expected == result


def test_extract_replication_info_non_classic_data():
    base_module = 'paasta_tools.monitoring'
    check_classic_module = base_module + '.check_classic_service_replication'
    extract_method = check_classic_module + '.extract_monitoring_info'
    mock_valid_non_classic_data = {
        'team': 'test_team',
        'service_type': 'not_classic',
    }
    with mock.patch(extract_method, return_value=mock_valid_non_classic_data, autospec=True):
        expected = (False, None)
        result = extract_replication_info({})
        assert expected == result


def test_extract_replication_info_valid_team_no_email():
    base_module = 'paasta_tools.monitoring'
    check_classic_module = base_module + '.check_classic_service_replication'
    extract_method = check_classic_module + '.extract_monitoring_info'
    mock_valid_team_no_email = {
        'team': 'test_team',
        'notification_email': None,
        'service_type': 'classic',
    }
    with mock.patch(extract_method, return_value=mock_valid_team_no_email, autospec=True):
        expected = (True, mock_valid_team_no_email)
        result = extract_replication_info({})
        assert expected == result


def test_extract_replication_info_invalid_data():
    base_module = 'paasta_tools.monitoring'
    check_classic_module = base_module + '.check_classic_service_replication'
    extract_method = check_classic_module + '.extract_monitoring_info'
    mock_invalid_data = {
        'team': None,
        'service_type': None,
    }
    with mock.patch(extract_method, return_value=mock_invalid_data, autospec=True):
        expected = (False, None)
        result = extract_replication_info({})
        assert expected == result


def test_classic_replication_check():
    base_module = 'paasta_tools.monitoring'
    check_classic_module = base_module + '.check_classic_service_replication'

    read_config_method = check_classic_module + '.read_services_configuration'
    replication_method = check_classic_module + '.get_replication_for_services'
    extract_method = check_classic_module + '.extract_replication_info'
    check_method = check_classic_module + '.do_replication_check'
    load_system_paasta_config_module = check_classic_module + '.load_system_paasta_config'

    mock_service_config = {'pow': {'wat': 1}}
    mock_replication = {'pow': -1}
    mock_monitoring = {'pow': 'bar'}
    mock_check = {
        'team': 'testing',
        'output': 'testing looks good',
        'runbook': 'fake-runbook',
        'status': 'fake-status',
        'name': 'fake-name',
    }

    with mock.patch(
        read_config_method, return_value=mock_service_config, autospec=True,
    ), mock.patch(
        replication_method, return_value=mock_replication, autospec=True,
    ), mock.patch(
        extract_method, return_value=(True, mock_monitoring), autospec=True,
    ), mock.patch(
        check_method, return_value=mock_check, autospec=True,
    ) as mcheck, mock.patch(
        'pysensu_yelp.send_event', autospec=True,
    ), mock.patch.object(
        sys, 'argv', ['check_classic_service_replication.py'],
    ), mock.patch(
        load_system_paasta_config_module,
        return_value=SystemPaastaConfig({}, '/fake/config'), autospec=True,
    ):
        with pytest.raises(SystemExit) as error:
            ClassicServiceReplicationCheck()
        mcheck.assert_called_with('pow', {'pow': 'bar'}, 0)
        assert error.value.code == 0


def test_classic_replication_check_connectionerror():
    with mock.patch(
        'paasta_tools.monitoring.check_classic_service_replication.get_replication_for_services',
        autospec=True,
    ) as mock_get_replication_for_services, mock.patch(
        'paasta_tools.monitoring.check_classic_service_replication.ClassicServiceReplicationCheck.__init__',
        autospec=True,
    ) as mock_init:
        mock_get_replication_for_services.side_effect = requests.exceptions.ConnectionError
        mock_init.return_value = None
        check = ClassicServiceReplicationCheck()
        check.critical = mock.Mock()
        check.get_service_replication(['this', 'that'], 'localhost', 12345, DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT)
        check.critical.assert_called_once_with('Failed to connect synapse haproxy on localhost:12345')


def test_classic_replication_check_unknownexception():
    with mock.patch(
        'paasta_tools.monitoring.check_classic_service_replication.get_replication_for_services',
        autospec=True,
    ) as mock_get_replication_for_services, mock.patch(
        'paasta_tools.monitoring.check_classic_service_replication.ClassicServiceReplicationCheck.__init__',
        autospec=True,
    ) as mock_init:
        mock_get_replication_for_services.side_effect = Exception
        mock_init.return_value = None
        check = ClassicServiceReplicationCheck()
        check.critical = mock.Mock()
        check.get_service_replication(['this', 'that'], 'localhost', 12345, DEFAULT_SYNAPSE_HAPROXY_URL_FORMAT)
        check.critical.assert_called_once_with(mock.ANY)
