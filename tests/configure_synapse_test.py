import contextlib

import mock
import pytest

from paasta_tools import configure_synapse


@pytest.yield_fixture
def mock_get_current_location():
    def f(typ):
        return {
            'region': 'my_region',
        }[typ]
    with mock.patch('paasta_tools.configure_synapse.get_current_location',
                    side_effect=f):
        yield


def test_get_zookeeper_topology():
    m = mock.mock_open()
    with contextlib.nested(
            mock.patch('paasta_tools.configure_synapse.open', m, create=True),
            mock.patch('yaml.load', return_value=[['foo', 42]])):
        zk_topology = configure_synapse.get_zookeeper_topology('/path/to/fake/file')
    assert zk_topology == ['foo:42']
    m.assert_called_with('/path/to/fake/file')


def test_generate_configuration(mock_get_current_location):
    actual_configuration = configure_synapse.generate_configuration(
        synapse_tools_config=configure_synapse.set_defaults({'bind_addr': '0.0.0.0'}),
        zookeeper_topology=['1.2.3.4', '2.3.4.5'],
        services=[
            (
                'test_service',
                {
                    'proxy_port': 1234,
                    'healthcheck_uri': '/status',
                    'retries': 2,
                    'timeout_connect_ms': 2000,
                    'timeout_server_ms': 3000,
                    'extra_headers': {
                        'X-Mode': 'ro'
                    },
                    'extra_healthcheck_headers': {
                        'X-Mode': 'ro'
                    },
                    'balance': 'roundrobin',
                }
            )
        ]
    )

    expected_configuration = configure_synapse.generate_base_config(
        synapse_tools_config=configure_synapse.set_defaults({'bind_addr': '0.0.0.0'})
    )
    expected_configuration['services'] = {
        'test_service': {
            'default_servers': [],
            'use_previous_backends': False,
            'discovery': {
                'hosts': ['1.2.3.4', '2.3.4.5'],
                'method': 'zookeeper',
                'path': '/nerve/region:my_region/test_service'},
            'haproxy': {
                'listen': [
                    'option httpchk GET /http/test_service/0/status HTTP/1.1\\r\\nX-Mode:\\ ro',
                    'http-check send-state',
                    'retries 2',
                    'timeout connect 2000ms',
                    'timeout server 3000ms',
                    'balance roundrobin',
                ],
                'frontend': [
                    'timeout client 3000ms',
                    'capture request header X-B3-SpanId len 64',
                    'capture request header X-B3-TraceId len 64',
                    'capture request header X-B3-ParentSpanId len 64',
                    'capture request header X-B3-Flags len 10',
                    'capture request header X-B3-Sampled len 10',
                    'option httplog',
                ],
                'backend': [
                    'reqidel ^X-Mode:.*',
                    'reqadd X-Mode:\ ro',
                ],
                'port': '1234',
                'server_options': 'check port 6666 observe layer7'
            }
        }
    }

    assert actual_configuration == expected_configuration


def test_generate_configuration_empty():
    actual_configuration = configure_synapse.generate_configuration(
        synapse_tools_config=configure_synapse.set_defaults({'bind_addr': '0.0.0.0'}),
        zookeeper_topology=['1.2.3.4', '2.3.4.5'],
        services=[]
    )
    expected_configuration = configure_synapse.generate_base_config(
        synapse_tools_config=configure_synapse.set_defaults({'bind_addr': '0.0.0.0'})
    )
    assert actual_configuration == expected_configuration


@contextlib.contextmanager
def setup_mocks_for_main():
    mock_tmp_file = mock.MagicMock()
    mock_file_cmp = mock.Mock()
    mock_copy = mock.Mock()
    mock_subprocess_check_call = mock.Mock()

    with contextlib.nested(
            mock.patch('paasta_tools.configure_synapse.get_zookeeper_topology'),
            mock.patch('paasta_tools.marathon_tools.get_all_namespaces'),
            mock.patch('paasta_tools.configure_synapse.generate_configuration'),
            mock.patch(
                'paasta_tools.configure_synapse.get_config',
                return_value=configure_synapse.set_defaults(
                    {'bind_addr': '0.0.0.0', 'config_file': '/etc/synapse/synapse.conf.json'}
                ),
            ),
            mock.patch('tempfile.NamedTemporaryFile', return_value=mock_tmp_file),
            mock.patch('paasta_tools.configure_synapse.open', create=True),
            mock.patch('json.dump'),
            mock.patch('os.chmod'),
            mock.patch('filecmp.cmp', mock_file_cmp),
            mock.patch('shutil.copy', mock_copy),
            mock.patch('subprocess.check_call', mock_subprocess_check_call)):
        yield(mock_tmp_file, mock_file_cmp, mock_copy, mock_subprocess_check_call)


def test_synapse_restarted_when_config_files_differ():
    with setup_mocks_for_main() as (
            mock_tmp_file, mock_file_cmp, mock_copy, mock_subprocess_check_call):

        # New and existing synapse configs differ
        mock_file_cmp.return_value = False

        configure_synapse.main(args=[])

        mock_copy.assert_called_with(
            mock_tmp_file.__enter__().name, '/etc/synapse/synapse.conf.json')
        mock_subprocess_check_call.assert_called_with(['service', 'synapse', 'restart'])


def test_synapse_not_restarted_when_config_files_are_identical():
    with setup_mocks_for_main() as (
            mock_tmp_file, mock_file_cmp, mock_copy, mock_subprocess_check_call):

        # New and existing synapse configs are identical
        mock_file_cmp.return_value = True

        configure_synapse.main(args=[])

        mock_copy.assert_called_with(
            mock_tmp_file.__enter__().name, '/etc/synapse/synapse.conf.json')
        assert not mock_subprocess_check_call.called


def test_chaos_delay(mock_get_current_location):
    with mock.patch.object(configure_synapse, 'get_my_grouping') as grouping_mock:
        grouping_mock.return_value = 'my_ecosystem'
        actual_configuration = configure_synapse.generate_configuration(
            synapse_tools_config=configure_synapse.set_defaults({'bind_addr': '0.0.0.0'}),
            zookeeper_topology=['1.2.3.4'],
            services=[
                (
                    'test_service',
                    {
                        'proxy_port': 1234,
                        'chaos': {'ecosystem': {'my_ecosystem': {'delay': '300ms'}}}
                    }
                )
            ]
        )
        grouping_mock.assert_called_once_with('ecosystem')
    frontend = actual_configuration['services']['test_service']['haproxy']['frontend']
    assert 'tcp-request inspect-delay 300ms' in frontend
    assert 'tcp-request content accept if WAIT_END' in frontend


def test_chaos_drop(mock_get_current_location):
    with mock.patch.object(configure_synapse, 'get_my_grouping') as grouping_mock:
        grouping_mock.return_value = 'my_ecosystem'
        actual_configuration = configure_synapse.generate_configuration(
            synapse_tools_config=configure_synapse.set_defaults({'bind_addr': '0.0.0.0'}),
            zookeeper_topology=['1.2.3.4'],
            services=[
                (
                    'test_service',
                    {
                        'proxy_port': 1234,
                        'chaos': {'ecosystem': {'my_ecosystem': {'fail': 'drop'}}}
                    }
                )
            ]
        )
        grouping_mock.assert_called_once_with('ecosystem')
    frontend = actual_configuration['services']['test_service']['haproxy']['frontend']
    assert 'tcp-request content reject' in frontend


def test_chaos_error_503(mock_get_current_location):
    with mock.patch.object(configure_synapse, 'get_my_grouping') as grouping_mock:
        grouping_mock.return_value = 'my_ecosystem'
        actual_configuration = configure_synapse.generate_configuration(
            synapse_tools_config=configure_synapse.set_defaults({'bind_addr': '0.0.0.0'}),
            zookeeper_topology=['1.2.3.4'],
            services=[
                (
                    'test_service',
                    {
                        'proxy_port': 1234,
                        'chaos': {'ecosystem': {'my_ecosystem': {'fail': 'error_503'}}}
                    }
                )
            ]
        )
        assert actual_configuration['services']['test_service']['discovery']['method'] == 'base'
