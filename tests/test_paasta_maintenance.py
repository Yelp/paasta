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
import mock

from paasta_tools import paasta_maintenance


@mock.patch('paasta_tools.mesos_maintenance.is_host_drained', autospec=True)
@mock.patch('paasta_tools.mesos_maintenance.get_hosts_past_maintenance_start', autospec=True)
def test_is_safe_to_kill(
    mock_get_hosts_past_maintenance_start,
    mock_is_host_drained,
):
    mock_is_host_drained.return_value = False
    mock_get_hosts_past_maintenance_start.return_value = []
    assert not paasta_maintenance.is_safe_to_kill('blah')

    mock_is_host_drained.return_value = False
    mock_get_hosts_past_maintenance_start.return_value = ['blah']
    assert paasta_maintenance.is_safe_to_kill('blah')

    mock_is_host_drained.return_value = True
    mock_get_hosts_past_maintenance_start.return_value = ['blah']
    assert paasta_maintenance.is_safe_to_kill('blah')

    mock_is_host_drained.return_value = True
    mock_get_hosts_past_maintenance_start.return_value = []
    assert paasta_maintenance.is_safe_to_kill('blah')


@mock.patch('paasta_tools.paasta_maintenance.is_hostname_local', autospec=True)
def test_is_safe_to_drain_rejects_non_localhosts(
    mock_is_hostname_local,
):
    mock_is_hostname_local.return_value = False
    assert paasta_maintenance.is_safe_to_drain('non-localhost') is False


@mock.patch('paasta_tools.paasta_maintenance.getfqdn', autospec=True)
@mock.patch('paasta_tools.paasta_maintenance.gethostname', autospec=True)
def test_is_hostname_local_works(
    mock_gethostname,
    mock_getfqdn,
):
    mock_gethostname.return_value = 'foo'
    mock_getfqdn.return_value = 'foo.bar'
    assert paasta_maintenance.is_hostname_local('localhost') is True
    assert paasta_maintenance.is_hostname_local('foo') is True
    assert paasta_maintenance.is_hostname_local('foo.bar') is True
    assert paasta_maintenance.is_hostname_local('something_different') is False


@mock.patch('paasta_tools.paasta_maintenance.utils.load_system_paasta_config', autospec=True)
def test_are_local_tasks_in_danger_fails_safe_with_false(
    mock_load_system_paasta_config,
):
    """If something unexpected happens that we don't know how to
    interpret, we make sure that we fail with "False" so that processes
    move on and don't deadlock. In general the answer to "is it safe to drain"
    is "yes" if mesos can't be reached, etc"""
    mock_load_system_paasta_config.side_effect = Exception
    assert paasta_maintenance.are_local_tasks_in_danger() is False


@mock.patch('paasta_tools.paasta_maintenance.utils.load_system_paasta_config', autospec=True)
@mock.patch('paasta_tools.paasta_maintenance.marathon_services_running_here', autospec=True)
def test_are_local_tasks_in_danger_is_false_with_nothing_running(
    mock_marathon_services_running_here,
    mock_load_system_paasta_config,
):
    mock_marathon_services_running_here.return_value = []
    assert paasta_maintenance.are_local_tasks_in_danger() is False


@mock.patch('paasta_tools.paasta_maintenance.utils.load_system_paasta_config', autospec=True)
@mock.patch('paasta_tools.paasta_maintenance.marathon_services_running_here', autospec=True)
@mock.patch('paasta_tools.paasta_maintenance.get_backends', autospec=True)
@mock.patch('paasta_tools.paasta_maintenance.is_healthy_in_haproxy', autospec=True)
def test_are_local_tasks_in_danger_is_false_with_an_unhealthy_service(
    mock_is_healthy_in_haproxy,
    mock_get_backends,
    mock_marathon_services_running_here,
    mock_load_system_paasta_config,
):
    mock_is_healthy_in_haproxy.return_value = False
    mock_marathon_services_running_here.return_value = [("service", "instance", 42)]
    assert paasta_maintenance.are_local_tasks_in_danger() is False
    mock_is_healthy_in_haproxy.assert_called_once_with(42, mock.ANY)


@mock.patch('paasta_tools.paasta_maintenance.utils.load_system_paasta_config', autospec=True)
@mock.patch('paasta_tools.paasta_maintenance.marathon_services_running_here', autospec=True)
@mock.patch('paasta_tools.paasta_maintenance.get_backends', autospec=True)
@mock.patch('paasta_tools.paasta_maintenance.is_healthy_in_haproxy', autospec=True)
@mock.patch('paasta_tools.paasta_maintenance.synapse_replication_is_low', autospec=True)
def test_are_local_tasks_in_danger_is_true_with_an_healthy_service_in_danger(
    mock_synapse_replication_is_low,
    mock_is_healthy_in_haproxy,
    mock_get_backends,
    mock_marathon_services_running_here,
    mock_load_system_paasta_config,
):
    mock_is_healthy_in_haproxy.return_value = True
    mock_synapse_replication_is_low.return_value = True
    mock_marathon_services_running_here.return_value = [("service", "instance", 42)]
    assert paasta_maintenance.are_local_tasks_in_danger() is True
    mock_is_healthy_in_haproxy.assert_called_once_with(42, mock.ANY)
    assert mock_synapse_replication_is_low.call_count == 1


@mock.patch('paasta_tools.paasta_maintenance.read_registration_for_service_instance', autospec=True)
@mock.patch('paasta_tools.paasta_maintenance.load_smartstack_info_for_service', autospec=True)
@mock.patch('paasta_tools.paasta_maintenance.get_expected_instance_count_for_namespace', autospec=True)
@mock.patch('paasta_tools.paasta_maintenance.get_replication_for_services', autospec=True)
def test_synapse_replication_is_low_understands_underreplicated_services(
    mock_get_replication_for_services,
    mock_get_expected_instance_count_for_namespace,
    mock_load_smartstack_info_for_service,
    mock_read_registration_for_service_instance,
):
    mock_read_registration_for_service_instance.return_value = "service.main"
    mock_get_expected_instance_count_for_namespace.return_value = 3
    mock_load_smartstack_info_for_service.return_value = {
        "local_region": {"service.main": "up"}
    }
    mock_get_replication_for_services.return_value = {"service.main": 1}
    local_backends = ["foo"]
    system_paasta_config = mock.MagicMock()
    assert paasta_maintenance.synapse_replication_is_low(
        service='service',
        instance='instance',
        system_paasta_config=system_paasta_config,
        local_backends=local_backends,
    ) is True


@mock.patch('paasta_tools.paasta_maintenance.gethostbyname', autospec=True)
def test_is_healthy_in_harproxy_healthy_path(
    mock_gethostbyname
):
    mock_gethostbyname.return_value = '192.0.2.1'
    local_port = 42
    backends = [
        {'status': 'UP', 'pxname': 'service.main', 'svname': '192.0.2.1:42_hostname'}
    ]
    assert paasta_maintenance.is_healthy_in_haproxy(
        local_port=local_port,
        backends=backends,
    ) is True


@mock.patch('paasta_tools.paasta_maintenance.gethostbyname', autospec=True)
def test_is_healthy_in_haproxy_unhealthy_path(
    mock_gethostbyname
):
    mock_gethostbyname.return_value = '192.0.2.1'
    local_port = 42
    backends = [
        {'status': 'DOWN', 'pxname': 'service.main', 'svname': '192.0.2.1:42_hostname'}
    ]
    assert paasta_maintenance.is_healthy_in_haproxy(
        local_port=local_port,
        backends=backends,
    ) is False


@mock.patch('paasta_tools.paasta_maintenance.gethostbyname', autospec=True)
def test_is_healthy_in_haproxy_missing_backend_entirely(
    mock_gethostbyname
):
    mock_gethostbyname.return_value = '192.0.2.1'
    local_port = 42
    backends = [
        {'status': 'DOWN', 'pxname': 'service.main', 'svname': '192.0.2.4:666_otherhostname'}
    ]
    assert paasta_maintenance.is_healthy_in_haproxy(
        local_port=local_port,
        backends=backends,
    ) is False
