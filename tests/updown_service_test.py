import contextlib
import os

import mock
import pytest
from requests.exceptions import RequestException

from paasta_tools import updown_service


def test_get_args_pass():
    tests = [
        [['updown_service', 'myservice.name', 'up'], 'up', None, 300, False],
        [['updown_service', 'myservice.name', 'down'], 'down', None, 300, False],
        [['updown_service', 'myservice.name', 'down', '-t', '42'], 'down', 42, 42, False],
        [['updown_service', 'myservice.name', 'down', '-x'], 'down', None, 300, True],
    ]

    for test in tests:
        argv, expected_state, expected_args_timeout, expected_timeout, expected_wait_only = test

        with mock.patch('sys.argv', argv):
            args = updown_service.get_args()
            timeout = updown_service._get_timeout_s(args.service, args.timeout)

        assert args.service == 'myservice.name'
        assert args.state == expected_state
        assert args.timeout == expected_args_timeout
        assert timeout == expected_timeout
        assert args.wait_only == expected_wait_only


def test_get_args_fail():
    tests = [
        ['updown_service'],
        ['updown_service', 'myservice.name'],
        ['updown_service', 'myservice', 'up'],
        ['updown_service', 'myservice.name', 'wibble'],
    ]

    for argv in tests:
        with mock.patch('sys.argv', argv):
            with pytest.raises(SystemExit) as excinfo:
                updown_service.get_args()
            assert str(excinfo.value) == '2', argv


def test_check_haproxy_state():
    tests = [
        # Up
        ['10.0.0.1', 'up', True],
        ['10.0.0.1', 'down', False],
        # Down
        ['10.0.0.2', 'up', False],
        ['10.0.0.2', 'down', True],
        # Maintenance
        ['10.0.0.3', 'up', False],
        ['10.0.0.3', 'down', True],
        # Missing
        ['10.0.0.4', 'up', False],
        ['10.0.0.4', 'down', True],
    ]

    mock_stats_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), 'haproxy_stats.csv')

    for test in tests:
        my_ip_address, expected_state, expected_result = test

        with open(mock_stats_path) as fd:
            with contextlib.nested(
                    mock.patch('urllib2.urlopen', return_value=fd),
                    mock.patch('paasta_tools.updown_service.get_my_ip_address',
                               return_value=my_ip_address)):
                actual_result = updown_service.check_haproxy_state(
                    'service_three.main', expected_state)

        assert actual_result == expected_result, test


def test_wait_for_haproxy_with_healthcheck_pass_returns_zero():
    mock_stats_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), 'haproxy_stats.csv')
    with open(mock_stats_path) as fd:
        with contextlib.nested(
                mock.patch('urllib2.urlopen', return_value=fd),
                mock.patch('subprocess.check_call', side_effect=Exception()),
                mock.patch('paasta_tools.updown_service.check_local_healthcheck',
                           return_value=True)) as (_, _, _):
            assert 0 == updown_service.wait_for_haproxy_state(
                'service_three.main', 'up', 10, 1)


def test_wait_for_haproxy_with_healthcheck_fail_returns_one():
    mock_stats_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), 'haproxy_stats.csv')

    with contextlib.nested(
            mock.patch('urllib2.urlopen', side_effect=lambda _, timeout: open(mock_stats_path)),
            mock.patch('time.sleep'),
            mock.patch('subprocess.check_call', side_effect=Exception()),
            mock.patch('paasta_tools.updown_service.check_local_healthcheck',
                       return_value=False)) as (_, _, _, _):
        assert 1 == updown_service.wait_for_haproxy_state(
            'service_three.main', 'up', 10, 1)


def test_check_local_healthcheck_returns_true_on_success():
    with contextlib.nested(
            mock.patch('paasta_tools.updown_service.read_service_configuration',
                       return_value={'port': 1010}),
            mock.patch('requests.get',
                       return_value=mock.Mock())) as (_, mock_http):
        assert updown_service.check_local_healthcheck(
            'service_three.main')
        mock_http.assert_called_once_with('http://127.0.0.1:1010/status')


def test_check_local_healthcheck_returns_false_on_failure():
    mock_get = mock.Mock(
        raise_for_status=mock.Mock(side_effect=RequestException()))
    with contextlib.nested(
            mock.patch('paasta_tools.updown_service.read_service_configuration',
                       return_value={'port': 1010}),
            mock.patch('requests.get',
                       return_value=mock_get)) as (_, mock_http):
        assert not updown_service.check_local_healthcheck(
            'service_three.main')
        mock_http.assert_called_once_with('http://127.0.0.1:1010/status')


def test_unknown_service():
    mock_stats_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), 'haproxy_stats.csv')

    with open(mock_stats_path) as fd:
        with contextlib.nested(
                mock.patch('urllib2.urlopen', return_value=fd),
                mock.patch('paasta_tools.updown_service.get_my_ip_address',
                           return_value='127.0.0.1'),
                mock.patch('sys.exit')) as (_, _, mock_exit):
            updown_service.check_haproxy_state('unknown_service', True)
            mock_exit.assert_called_once_with(1)


def test_wait_for_haproxy_state():
    tests = [
        # Service is immediately in the expected state
        [[True], 0, 1],
        # Service never enters the expected state
        [10 * [False], 1, 10],
        # Service enters the expected state on third poll
        [[False, False, True], 0, 3],
    ]

    for test in tests:
        mock_check_haproxy_state, expected_result, expected_mock_sleep_call_count = test

        with contextlib.nested(
                mock.patch('time.sleep'),
                mock.patch('subprocess.check_call'),
                mock.patch('paasta_tools.updown_service.check_haproxy_state',
                           side_effect=mock_check_haproxy_state)) as (mock_sleep, _, _):
            actual_result = updown_service.wait_for_haproxy_state(
                'service_three.main', 'down', 10, 1)

        assert expected_result == actual_result
        assert mock_sleep.call_count == expected_mock_sleep_call_count


def test_wait_for_haproxy_state_handles_timeout_0():
    actual_result = updown_service.wait_for_haproxy_state(
        service='service_three.main',
        expected_state='down',
        timeout=0,
        wait_time=1)
    assert actual_result == 1


def test_should_manage_service():
    mconfig_path = 'paasta_tools.updown_service.load_service_namespace_config'
    mconfig = mock.Mock(return_value={'proxy_port': 3})

    sconfig_path = 'paasta_tools.updown_service.read_service_configuration'
    with contextlib.nested(
            mock.patch(mconfig_path, new=mconfig),
            mock.patch(sconfig_path, return_value={})):
        assert updown_service._should_manage_service('test.main')

    with contextlib.nested(
            mock.patch(mconfig_path, new=mconfig),
            mock.patch(sconfig_path, return_value={'no_updown_service': True})):
        assert not updown_service._should_manage_service('test.main')

    mconfig.return_value = {}
    with contextlib.nested(
            mock.patch(mconfig_path, new=mconfig),
            mock.patch(sconfig_path, return_value={})):
        assert not updown_service._should_manage_service('test.main')


def test_timeout_s():
    arg_timeout_s = 30
    new_timeout_s = 50
    mconfig_path = 'paasta_tools.updown_service.load_service_namespace_config'

    mconfig = mock.Mock(return_value={})
    with mock.patch(mconfig_path, new=mconfig):
        assert updown_service._get_timeout_s('test.main', arg_timeout_s) == arg_timeout_s

    mconfig = mock.Mock(return_value={})
    with mock.patch(mconfig_path, new=mconfig):
        assert updown_service._get_timeout_s('test.main', None) == updown_service.DEFAULT_TIMEOUT_S

    mconfig = mock.Mock(return_value={'updown_timeout_s': new_timeout_s})
    with mock.patch(mconfig_path, new=mconfig):
        assert updown_service._get_timeout_s('test.main', arg_timeout_s) == arg_timeout_s

    mconfig = mock.Mock(return_value={'updown_timeout_s': new_timeout_s})
    with mock.patch(mconfig_path, new=mconfig):
        assert updown_service._get_timeout_s('test.main', None) == new_timeout_s
