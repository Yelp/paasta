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
from bravado.exception import HTTPError
from mock import ANY
from mock import Mock
from mock import patch
from pytest import raises

from paasta_tools.cli.cmds import mark_for_deployment
from paasta_tools.utils import TimeoutError


class fake_args:
    deploy_group = 'test_deploy_group'
    service = 'test_service'
    git_url = 'git://false.repo/services/test_services'
    commit = 'fake-hash'
    soa_dir = 'fake_soa_dir'
    block = False
    verbose = False


@patch('paasta_tools.cli.cmds.mark_for_deployment.validate_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment', autospec=True)
@patch('paasta_tools.cli.cmds.mark_for_deployment.get_currently_deployed_sha', autospec=True)
def test_paasta_mark_for_deployment_acts_like_main(
    mock_get_currently_deployed_sha,
    mock_mark_for_deployment,
    mock_validate_service_name,
):
    mock_mark_for_deployment.return_value = 42
    assert mark_for_deployment.paasta_mark_for_deployment(fake_args) == 42
    mock_mark_for_deployment.assert_called_once_with(
        service='test_service',
        deploy_group='test_deploy_group',
        commit='fake-hash',
        git_url='git://false.repo/services/test_services',
    )
    assert mock_validate_service_name.called


@patch('paasta_tools.cli.cmds.mark_for_deployment._log', autospec=True)
@patch('paasta_tools.remote_git.create_remote_refs', autospec=True)
def test_mark_for_deployment_happy(mock_create_remote_refs, mock__log):
    actual = mark_for_deployment.mark_for_deployment(
        git_url='fake_git_url',
        deploy_group='fake_deploy_group',
        service='fake_service',
        commit='fake_commit',
    )
    assert actual == 0
    mock_create_remote_refs.assert_called_once_with(
        git_url='fake_git_url',
        ref_mutator=ANY,
        force=True,
    )


@patch('paasta_tools.cli.cmds.mark_for_deployment._log', autospec=True)
@patch('paasta_tools.remote_git.create_remote_refs', autospec=True)
def test_mark_for_deployment_sad(mock_create_remote_refs, mock__log):
    mock_create_remote_refs.side_effect = Exception('something bad')
    actual = mark_for_deployment.mark_for_deployment(
        git_url='fake_git_url',
        deploy_group='fake_deploy_group',
        service='fake_service',
        commit='fake_commit',
    )
    assert actual == 1
    mock_create_remote_refs.assert_called_once_with(
        git_url='fake_git_url',
        ref_mutator=ANY,
        force=True,
    )


def mock_status_instance_side_effect(service, instance):
    if instance in ['instance1', 'instance6', 'notaninstance', 'api_error']:
        # valid completed instance
        mock_mstatus = Mock(app_count=1, deploy_status='Running',
                            expected_instance_count=2,
                            running_instance_count=2)
    if instance == 'instance2':
        # too many marathon apps
        mock_mstatus = Mock(app_count=2, deploy_status='Running',
                            expected_instance_count=2,
                            running_instance_count=2)
    if instance == 'instance3':
        # too many running instances
        mock_mstatus = Mock(app_count=1, deploy_status='Running',
                            expected_instance_count=2,
                            running_instance_count=4)
    if instance == 'instance4':
        # still Deploying
        mock_mstatus = Mock(app_count=1, deploy_status='Deploying',
                            expected_instance_count=2,
                            running_instance_count=2)
    if instance == 'instance5':
        # not a marathon instance
        mock_mstatus = None
    if instance == 'instance7':
        # paasta stop'd
        mock_mstatus = Mock(app_count=1, deploy_status='Stopped',
                            expected_instance_count=0,
                            running_instance_count=0,
                            desired_state='stop')
    if instance == 'instance8':
        # paasta has autoscaled to 0
        mock_mstatus = Mock(app_count=1, deploy_status='Stopped',
                            expected_instance_count=0,
                            running_instance_count=0)
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
        mock_status_instance.result.side_effect = HTTPError(response=Mock(status_code=404))
    if instance == 'api_error':
        # not an instance paasta can find
        mock_status_instance.result.side_effect = HTTPError(response=Mock(status_code=500))
    return mock_status_instance


@patch('paasta_tools.cli.cmds.mark_for_deployment._log', autospec=True)
@patch('paasta_tools.cli.cmds.mark_for_deployment.client.get_paasta_api_client', autospec=True)
def test_instances_deployed(mock_get_paasta_api_client, mock__log):
    mock_paasta_api_client = Mock()
    mock_get_paasta_api_client.return_value = mock_paasta_api_client
    mock_paasta_api_client.service.status_instance.side_effect = mock_status_instance_side_effect

    assert mark_for_deployment.instances_deployed('cluster', 'service1', ['instance1'], 'somesha') == 1
    assert mark_for_deployment.instances_deployed('cluster', 'service1', ['instance2', 'instance1'], 'somesha') == 1
    assert mark_for_deployment.instances_deployed('cluster', 'service1', ['instance3'], 'somesha') == 0
    assert mark_for_deployment.instances_deployed('cluster', 'service1', ['instance4'], 'somesha') == 0
    assert mark_for_deployment.instances_deployed('cluster', 'service1', ['instance5', 'instance1'], 'somesha') == 2
    assert mark_for_deployment.instances_deployed('cluster', 'service1', ['instance6'], 'somesha') == 0
    assert mark_for_deployment.instances_deployed('cluster', 'service1', ['notaninstance'], 'somesha') == 0
    assert mark_for_deployment.instances_deployed('cluster', 'service1', ['api_error'], 'somesha') == 0
    assert mark_for_deployment.instances_deployed('cluster', 'service1', ['instance7'], 'somesha') == 1
    assert mark_for_deployment.instances_deployed('cluster', 'service1', ['instance8'], 'somesha') == 1


def instances_deployed_side_effect(cluster, service, instances, git_sha):
    if instances == ['instance1', 'instance2']:
        return 2
    return 0


@patch('paasta_tools.cli.cmds.mark_for_deployment.get_cluster_instance_map_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.mark_for_deployment._log', autospec=True)
@patch('paasta_tools.cli.cmds.mark_for_deployment.instances_deployed', autospec=True)
@patch('time.sleep', autospec=True)
def test_wait_for_deployment(mock_sleep, mock_instances_deployed, mock__log,
                             mock_get_cluster_instance_map_for_service):
    mock_cluster_map = {'cluster1': {'instances': ['instance1', 'instance2', 'instance3']}}
    mock_get_cluster_instance_map_for_service.return_value = mock_cluster_map

    mock_instances_deployed.side_effect = instances_deployed_side_effect
    mock_sleeper = MockSleep()
    mock_sleep.side_effect = mock_sleeper.mock_sleep_side_effect

    with raises(TimeoutError):
        mark_for_deployment.wait_for_deployment('service', 'deploy_group_1', 'somesha', '/nail/soa', 1)
    mock_get_cluster_instance_map_for_service.assert_called_with('/nail/soa', 'service', 'deploy_group_1')
    mock_instances_deployed.assert_called_with(cluster='cluster1',
                                               service='service',
                                               instances=mock_cluster_map['cluster1']['instances'],
                                               git_sha='somesha')

    mock_cluster_map = {'cluster1': {'instances': ['instance1', 'instance2']},
                        'cluster2': {'instances': ['instance1', 'instance2']}}
    mock_get_cluster_instance_map_for_service.return_value = mock_cluster_map
    assert mark_for_deployment.wait_for_deployment('service', 'deploy_group_1', 'somesha', '/nail/soa', 1)

    mock_cluster_map = {'cluster1': {'instances': ['instance1', 'instance2']},
                        'cluster2': {'instances': ['instance1', 'instance3']}}
    mock_get_cluster_instance_map_for_service.return_value = mock_cluster_map
    mock_sleeper.call_count = 0
    with raises(TimeoutError):
        mark_for_deployment.wait_for_deployment('service', 'deploy_group_1', 'somesha', '/nail/soa', 1)


class MockSleep:
    def __init__(self):
        self.call_count = 0

    def mock_sleep_side_effect(self, time):
        if self.call_count == 5:
            raise TimeoutError()
        self.call_count += 1
        return
