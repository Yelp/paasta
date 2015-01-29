from contextlib import nested

import mock
import pytest
import requests
import sys
from StringIO import StringIO

from paasta_tools.monitoring.check_classic_service_replication\
    import (
        report_event,
        do_replication_check,
        extract_replication_info,
        ClassicServiceReplicationCheck
    )


def test_report_event():
    with mock.patch('pysensu_yelp.send_event') as mock_call:
        report_event({'team': 'search_infra'})
        mock_call.assert_called_with(team='search_infra')

        with pytest.raises(Exception):
            report_event({'foo': 'bar'})


def test_do_replication_check():
    base_module = 'paasta_tools.monitoring'
    check_classic_module = base_module + '.check_classic_service_replication'

    check_method = check_classic_module + '.check_replication'
    read_key_method = check_classic_module + '.read_key'

    mock_keys = ['team', 'notification_email', 'runbook', 'tip', 'page',
                 'alert_after', 'realert_every']

    mock_default_data = dict([(key, None) for key in mock_keys])
    mock_default_data['team'] = 'test_team'

    mock_specific_data = dict(
        [(key, "test_{0}".format(key)) for key in mock_keys]
    )
    mock_specific_data['extra'] = {
        'replication': {
            'key': 'test_key',
            'default': 'test_default',
            'map': 'test_map'
        }
    }

    with nested(
            mock.patch(check_method, return_value=(-1, 'bar')),
            mock.patch(read_key_method, return_value=-2)):
        expected = {
            'name': 'replication_test_service',
            'status': -1,
            'output': 'bar',
            'team': 'test_team',
            'notification_email': None,
            'runbook': 'no runbook',
            'tip': 'no tip',
            'page': False,
            'alert_after': '0s',
            'realert_every': -1
        }
        results = do_replication_check('test_service', mock_default_data,
                                       3)
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
            'alert_after': 'test_alert_after',
            'realert_every': 'test_realert_every'
        }
        results = do_replication_check('test_service', mock_specific_data,
                                       3)
        assert expected == results


def test_extract_replication_info_valid_data():
    base_module = 'paasta_tools.monitoring'
    check_classic_module = base_module + '.check_classic_service_replication'
    extract_method = check_classic_module + '.extract_monitoring_info'

    mock_valid_data = {
        'team': 'test_team',
        'service_type': 'classic'
    }
    with mock.patch(extract_method, return_value=mock_valid_data):
        expected = (True, mock_valid_data)
        result = extract_replication_info({})
        assert expected == result


def test_extract_replication_info_non_classic_data():
    base_module = 'paasta_tools.monitoring'
    check_classic_module = base_module + '.check_classic_service_replication'
    extract_method = check_classic_module + '.extract_monitoring_info'
    mock_valid_non_classic_data = {
        'team': 'test_team',
        'service_type': 'not_classic'
    }
    with mock.patch(extract_method, return_value=mock_valid_non_classic_data):
        expected = (False, {})
        result = extract_replication_info({})
        assert expected == result


def test_extract_replication_info_valid_team_no_email():
    base_module = 'paasta_tools.monitoring'
    check_classic_module = base_module + '.check_classic_service_replication'
    extract_method = check_classic_module + '.extract_monitoring_info'
    mock_valid_team_no_email = {
        'team': 'test_team',
        'notification_email': None,
        'service_type': 'classic'
    }
    with mock.patch(extract_method, return_value=mock_valid_team_no_email):
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
    with mock.patch(extract_method, return_value=mock_invalid_data):
        expected = (False, {})
        result = extract_replication_info({})
        assert expected == result


def test_classic_replication_check():
    base_module = 'paasta_tools.monitoring'
    check_classic_module = base_module + '.check_classic_service_replication'

    read_config_method = check_classic_module + '.read_services_configuration'
    replication_method = check_classic_module + '.get_replication_for_services'
    extract_method = check_classic_module + '.extract_replication_info'
    check_method = check_classic_module + '.do_replication_check'

    mock_service_config = {'pow': {'wat': 1}}
    mock_replication = {'pow': -1}
    mock_monitoring = {'pow': 'bar'}
    mock_check = {'team': 'testing', 'output': 'testing looks good'}

    with nested(
            mock.patch(read_config_method, return_value=mock_service_config),
            mock.patch(replication_method, return_value=mock_replication),
            mock.patch(extract_method, return_value=(True, mock_monitoring)),
            mock.patch(check_method, return_value=mock_check),
            mock.patch('pysensu_yelp.send_event')) as (_, _, _, mcheck, _):
        with pytest.raises(SystemExit) as error:
            check = ClassicServiceReplicationCheck()
            check.run()
            assert mcheck.assert_called_with('pow', {'wat': 1}, -1)
        assert error.value.code == 0

def test_classic_replication_check_connectionerror():
    base_module = 'paasta_tools.monitoring'
    check_classic_module = base_module + '.check_classic_service_replication'
    replication_method = check_classic_module + '.get_replication_for_services'

    SYNAPSE_HOST_PORT = "localhost:3212"
    connection_error_message = "ClassicServiceReplicationCheck CRITICAL: Failed to connect synapse haproxy on {0}".format(SYNAPSE_HOST_PORT)
    sensu_critical = 2

    with nested(
        mock.patch(replication_method, side_effect=requests.exceptions.ConnectionError),
        mock.patch('sys.stdout', new=StringIO()),
        pytest.raises(SystemExit)) as (_,fake_out,error):
            check = ClassicServiceReplicationCheck()
            check.run()
    assert fake_out.getvalue().strip() == connection_error_message
    assert error.value.code == sensu_critical

def test_classic_replication_check_unknownexception():
    base_module = 'paasta_tools.monitoring'
    check_classic_module = base_module + '.check_classic_service_replication'
    replication_method = check_classic_module + '.get_replication_for_services'

    sensu_critical = 2

    with nested(
            mock.patch(replication_method, side_effect=Exception),
            pytest.raises(SystemExit)) as (_,error):
        check = ClassicServiceReplicationCheck()
        check.run()
    assert error.value.code == sensu_critical
