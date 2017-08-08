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

from threading import Event

from bravado.exception import HTTPError
from mock import Mock
from mock import patch
from pytest import raises
from six.moves.queue import Queue

from paasta_tools.cli.cmds import mark_for_deployment
from paasta_tools.cli.cmds.mark_for_deployment import NoSuchCluster
from paasta_tools.cli.cmds.wait_for_deployment import get_latest_marked_sha
from paasta_tools.cli.cmds.wait_for_deployment import paasta_wait_for_deployment
from paasta_tools.cli.cmds.wait_for_deployment import validate_git_sha
from paasta_tools.cli.utils import NoSuchService
from paasta_tools.remote_git import LSRemoteException
from paasta_tools.utils import TimeoutError


class fake_args:
    deploy_group = 'test_deploy_group'
    service = 'test_service'
    git_url = ''
    commit = 'd670460b4b4aece5915caf5c68d12f560a9fe3e4'
    soa_dir = 'fake_soa_dir'
    timeout = 0
    verbose = False


def mock_status_instance_side_effect(service, instance):  # pragma: no cover (gevent)
    if instance in ['instance1', 'instance6', 'notaninstance', 'api_error']:
        # valid completed instance
        mock_mstatus = Mock(
            app_count=1, deploy_status='Running',
            expected_instance_count=2,
            running_instance_count=2,
        )
    if instance == 'instance2':
        # too many marathon apps
        mock_mstatus = Mock(
            app_count=2, deploy_status='Running',
            expected_instance_count=2,
            running_instance_count=2,
        )
    if instance == 'instance3':
        # too many running instances
        mock_mstatus = Mock(
            app_count=1, deploy_status='Running',
            expected_instance_count=2,
            running_instance_count=4,
        )
    if instance == 'instance4':
        # still Deploying
        mock_mstatus = Mock(
            app_count=1, deploy_status='Deploying',
            expected_instance_count=2,
            running_instance_count=2,
        )
    if instance == 'instance4.1':
        # still Deploying
        mock_mstatus = Mock(
            app_count=1, deploy_status='Waiting',
            expected_instance_count=2,
            running_instance_count=2,
        )
    if instance == 'instance5':
        # not a marathon instance
        mock_mstatus = None
    if instance == 'instance7':
        # paasta stop'd
        mock_mstatus = Mock(
            app_count=1, deploy_status='Stopped',
            expected_instance_count=0,
            running_instance_count=0,
            desired_state='stop',
        )
    if instance == 'instance8':
        # paasta has autoscaled to 0
        mock_mstatus = Mock(
            app_count=1, deploy_status='Stopped',
            expected_instance_count=0,
            running_instance_count=0,
        )
    mock_status = Mock()
    mock_status.git_sha = 'somesha'
    if instance == 'instance6':
        # running the wrong version
        mock_status.git_sha = 'anothersha'
    mock_status.marathon = mock_mstatus
    mock_result = mock_status
    mock_status_instance = Mock()
    mock_status_instance.result.return_value = mock_result
    if instance == 'notaninstance':
        # not an instance paasta can find
        mock_status_instance.result.side_effect = \
            HTTPError(response=Mock(status_code=404))
    if instance == 'api_error':
        # not an instance paasta can find
        mock_status_instance.result.side_effect = \
            HTTPError(response=Mock(status_code=500))
    return mock_status_instance


@patch('paasta_tools.cli.cmds.mark_for_deployment._log', autospec=True)
@patch(
    'paasta_tools.cli.cmds.mark_for_deployment.client.get_paasta_api_client',
    autospec=True,
)
def test_instances_deployed(mock_get_paasta_api_client, mock__log):
    mock_paasta_api_client = Mock()
    mock_get_paasta_api_client.return_value = mock_paasta_api_client
    mock_paasta_api_client.service.status_instance.side_effect = \
        mock_status_instance_side_effect

    f = mark_for_deployment.instances_deployed
    e = Event()
    e.set()
    cluster_data = mark_for_deployment.ClusterData(
        cluster='cluster',
        service='service1',
        git_sha='somesha',
        instances_queue=Queue(),
    )
    cluster_data.instances_queue.put('instance1')
    instances_out = Queue()
    f(cluster_data, instances_out, e)
    assert cluster_data.instances_queue.empty()
    assert instances_out.empty()

    cluster_data.instances_queue = Queue()
    cluster_data.instances_queue.put('instance1')
    cluster_data.instances_queue.put('instance2')
    instances_out = Queue()
    f(cluster_data, instances_out, e)
    assert cluster_data.instances_queue.empty()
    assert instances_out.get(block=False) == 'instance2'

    cluster_data.instances_queue = Queue()
    cluster_data.instances_queue.put('instance3')
    instances_out = Queue()
    f(cluster_data, instances_out, e)
    assert cluster_data.instances_queue.empty()
    assert instances_out.empty()

    cluster_data.instances_queue = Queue()
    cluster_data.instances_queue.put('instance4')
    instances_out = Queue()
    f(cluster_data, instances_out, e)
    assert cluster_data.instances_queue.empty()
    assert instances_out.empty()

    cluster_data.instances_queue = Queue()
    cluster_data.instances_queue.put('instance4.1')
    instances_out = Queue()
    f(cluster_data, instances_out, e)
    assert cluster_data.instances_queue.empty()
    assert instances_out.empty()

    cluster_data.instances_queue = Queue()
    cluster_data.instances_queue.put('instance5')
    cluster_data.instances_queue.put('instance1')
    instances_out = Queue()
    f(cluster_data, instances_out, e)
    assert cluster_data.instances_queue.empty()
    assert instances_out.empty()

    cluster_data.instances_queue = Queue()
    cluster_data.instances_queue.put('instance6')
    instances_out = Queue()
    f(cluster_data, instances_out, e)
    assert cluster_data.instances_queue.empty()
    assert instances_out.get(block=False) == 'instance6'

    cluster_data.instances_queue = Queue()
    cluster_data.instances_queue.put('notaninstance')
    instances_out = Queue()
    f(cluster_data, instances_out, e)
    assert cluster_data.instances_queue.empty()
    assert instances_out.get(block=False) == 'notaninstance'

    cluster_data.instances_queue = Queue()
    cluster_data.instances_queue.put('api_error')
    instances_out = Queue()
    f(cluster_data, instances_out, e)
    assert cluster_data.instances_queue.empty()
    assert instances_out.get(block=False) == 'api_error'

    cluster_data.instances_queue = Queue()
    cluster_data.instances_queue.put('instance7')
    instances_out = Queue()
    f(cluster_data, instances_out, e)
    assert cluster_data.instances_queue.empty()
    assert instances_out.empty()

    cluster_data.instances_queue = Queue()
    cluster_data.instances_queue.put('instance8')
    instances_out = Queue()
    f(cluster_data, instances_out, e)
    assert cluster_data.instances_queue.empty()
    assert instances_out.empty()


def instances_deployed_side_effect(cluster_data, instances_out, green_light):  # pragma: no cover (gevent)
    while not cluster_data.instances_queue.empty():
        instance = cluster_data.instances_queue.get()
        if instance not in ['instance1', 'instance2']:
            instances_out.put(instance)
        cluster_data.instances_queue.task_done()


@patch('paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config', autospec=True)
@patch('paasta_tools.cli.cmds.mark_for_deployment.get_cluster_instance_map_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.mark_for_deployment._log', autospec=True)
@patch('paasta_tools.cli.cmds.mark_for_deployment.instances_deployed', autospec=True)
def test_wait_for_deployment(
    mock_instances_deployed,
    mock__log,
    mock_get_cluster_instance_map_for_service,
    mock_load_system_paasta_config,
):
    mock_cluster_map = {'cluster1': {'instances': ['instance1', 'instance2', 'instance3']}}
    mock_get_cluster_instance_map_for_service.return_value = mock_cluster_map
    mock_instances_deployed.side_effect = instances_deployed_side_effect

    mock_load_system_paasta_config.return_value.get_api_endpoints.return_value = \
        {'cluster1': 'some_url_1', 'cluster2': 'some_url_2'}

    with raises(TimeoutError):
        with patch('time.time', side_effect=[0, 0, 2], autospec=True):
            with patch('time.sleep', autospec=True):
                mark_for_deployment.wait_for_deployment('service', 'deploy_group_1', 'somesha', '/nail/soa', 1)
    mock_get_cluster_instance_map_for_service.assert_called_with('/nail/soa', 'service', 'deploy_group_1')

    mock_cluster_map = {
        'cluster1': {'instances': ['instance1', 'instance2']},
        'cluster2': {'instances': ['instance1', 'instance2']},
    }
    mock_get_cluster_instance_map_for_service.return_value = mock_cluster_map
    with patch('sys.stdout', autospec=True, flush=Mock()):
        assert mark_for_deployment.wait_for_deployment('service', 'deploy_group_2', 'somesha', '/nail/soa', 5) == 0

    mock_cluster_map = {
        'cluster1': {'instances': ['instance1', 'instance2']},
        'cluster2': {'instances': ['instance1', 'instance3']},
    }
    mock_get_cluster_instance_map_for_service.return_value = mock_cluster_map
    with raises(TimeoutError):
        mark_for_deployment.wait_for_deployment('service', 'deploy_group_3', 'somesha', '/nail/soa', 0)


@patch('paasta_tools.cli.cmds.mark_for_deployment.load_system_paasta_config', autospec=True)
@patch('paasta_tools.cli.cmds.mark_for_deployment.get_cluster_instance_map_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.mark_for_deployment._log', autospec=True)
@patch('paasta_tools.cli.cmds.mark_for_deployment.instances_deployed', autospec=True)
def test_wait_for_deployment_raise_no_such_cluster(
    mock_instances_deployed,
    mock__log,
    mock_get_cluster_instance_map_for_service,
    mock_load_system_paasta_config,
):
    mock_load_system_paasta_config.return_value.get_api_endpoints.return_value = \
        {'cluster1': 'some_url_1', 'cluster2': 'some_url_2'}

    mock_get_cluster_instance_map_for_service.return_value = {'cluster3': {'instances': ['instance3']}}
    with raises(NoSuchCluster):
        mark_for_deployment.wait_for_deployment('service', 'deploy_group_3', 'somesha', '/nail/soa', 0)


@patch('paasta_tools.cli.cmds.wait_for_deployment.validate_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment', autospec=True)
def test_paasta_wait_for_deployment_return_1_when_no_such_service(
    mock_wait_for_deployment,
    mock_validate_service_name,
):
    mock_validate_service_name.side_effect = NoSuchService('Some text')
    assert paasta_wait_for_deployment(fake_args) == 1
    assert mock_wait_for_deployment.call_args_list == []
    assert mock_validate_service_name.called


@patch('paasta_tools.cli.cmds.wait_for_deployment.validate_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.wait_for_deployment.list_deploy_groups', autospec=True)
@patch('paasta_tools.cli.cmds.mark_for_deployment.wait_for_deployment', autospec=True)
def test_paasta_wait_for_deployment_return_1_when_deploy_group_not_found(
    mock_wait_for_deployment,
    mock_list_deploy_groups,
    mock_validate_service_name,
):
    mock_list_deploy_groups.return_value = {'another_test_deploy_group'}
    assert paasta_wait_for_deployment(fake_args) == 1
    assert mock_wait_for_deployment.call_args_list == []
    assert mock_validate_service_name.called


@patch('paasta_tools.cli.cmds.mark_for_deployment.get_cluster_instance_map_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.wait_for_deployment.validate_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.wait_for_deployment.list_deploy_groups', autospec=True)
def test_paasta_wait_for_deployment_return_1_when_no_instances_in_deploy_group(
    mock_list_deploy_groups,
    mock_validate_service_name,
    mock_get_cluster_instance_map_for_service,
):
    mock_list_deploy_groups.return_value = {'test_deploy_group'}
    mock_get_cluster_instance_map_for_service.return_value = {}
    assert paasta_wait_for_deployment(fake_args) == 1
    assert mock_validate_service_name.called


@patch('paasta_tools.cli.cmds.wait_for_deployment.list_remote_refs', autospec=True)
def test_get_latest_marked_sha_good(mock_list_remote_refs):
    mock_list_remote_refs.return_value = {
        'refs/tags/paasta-fake_group1-20161129T203750-deploy':
            '968b948b3fca457326718dc7b2e278f89ccc5c87',
        'refs/tags/paasta-fake_group1-20161117T122449-deploy':
            'eac9a6d7909d09ffec00538bbc43b64502aa2dc0',
        'refs/tags/paasta-fake_group2-20161125T095651-deploy':
            'a4911648beb2e53886658ba7ea7eb93d582d754c',
        'refs/tags/paasta-fake_group1.everywhere-20161109T223959-deploy':
            '71e97ec397a3f0e7c4ee46e8ea1e2982cbcb0b79',
    }
    assert get_latest_marked_sha('', 'fake_group1') \
        == '968b948b3fca457326718dc7b2e278f89ccc5c87'


@patch('paasta_tools.cli.cmds.wait_for_deployment.list_remote_refs', autospec=True)
def test_get_latest_marked_sha_bad(mock_list_remote_refs):
    mock_list_remote_refs.return_value = {
        'refs/tags/paasta-fake_group2-20161129T203750-deploy':
            '968b948b3fca457326718dc7b2e278f89ccc5c87',
    }
    assert get_latest_marked_sha('', 'fake_group1') == ''


@patch('paasta_tools.cli.cmds.wait_for_deployment.list_remote_refs', autospec=True)
def test_validate_deploy_group_when_is_git_not_available(mock_list_remote_refs):
    test_error_message = 'Git error'
    mock_list_remote_refs.side_effect = LSRemoteException(test_error_message)
    assert validate_git_sha(
        'fake sha', 'fake_git_url',
        'fake_group', 'fake_service',
    ) is None


def test_compose_timeout_message():
    clusters_data = []
    clusters_data.append(mark_for_deployment.ClusterData(
        cluster='cluster1',
        service='someservice',
        git_sha='somesha',
        instances_queue=Queue(),
    ))
    clusters_data[0].instances_queue.put('instance1')
    clusters_data[0].instances_queue.put('instance2')
    clusters_data.append(mark_for_deployment.ClusterData(
        cluster='cluster2',
        service='someservice',
        git_sha='somesha',
        instances_queue=Queue(),
    ))
    clusters_data[1].instances_queue.put('instance3')
    clusters_data.append(mark_for_deployment.ClusterData(
        cluster='cluster3',
        service='someservice',
        git_sha='somesha',
        instances_queue=Queue(),
    ))
    message = mark_for_deployment.compose_timeout_message(
        clusters_data, 1, 'fake_group',
        'someservice', 'some_git_sha',
    )
    assert '  paasta status -c cluster1 -s someservice -i instance1,instance2' in message
    assert '  paasta status -c cluster2 -s someservice -i instance3' in message
    assert '  paasta logs -c cluster1 -s someservice -i instance1,instance2 -C deploy -l 1000' in message
    assert '  paasta logs -c cluster2 -s someservice -i instance3 -C deploy -l 1000' in message
