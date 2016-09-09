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


@patch('paasta_tools.cli.cmds.mark_for_deployment.validate_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.mark_for_deployment.mark_for_deployment', autospec=True)
def test_paasta_mark_for_deployment_acts_like_main(
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


def is_instance_deployed_side_effect(service, instance, sha):
    if instance in ['instance1', 'instance2']:
        return True
    return False


@patch('paasta_tools.cli.cmds.mark_for_deployment.client.get_paasta_api_client', autospec=True)
@patch('paasta_tools.cli.cmds.mark_for_deployment._log', autospec=True)
@patch('paasta_tools.cli.cmds.mark_for_deployment.is_instance_deployed')
@patch('time.sleep', autospec=True)
def test_wait_for_deployment(mock_sleep, mock_is_instance_deployed, mock__log, mock_get_paasta_api_client):
    mock_result = {'instances': ['instance1', 'instance2', 'instance3']}
    mock_list_instances = Mock()
    mock_list_instances.result.return_value = mock_result
    mock_paasta_api_client = Mock()
    mock_get_paasta_api_client.return_value = mock_paasta_api_client
    mock_paasta_api_client.service.list_instances.return_value = mock_list_instances

    mock_is_instance_deployed.side_effect = is_instance_deployed_side_effect
    mock_sleep.side_effect = TimeoutError()
    with raises(TimeoutError):
        mark_for_deployment.wait_for_deployment('service', 'deploy_group_1', 'somesha', '/nail/soa', 1)
    mock_paasta_api_client.service.list_instances.assert_called_with(service='service', deploy_group='deploy_group_1')
    mock_is_instance_deployed.assert_called_with('service', 'instance3', 'somesha')

    mock_result = {'instances': ['instance1', 'instance2']}
    mock_list_instances.result.return_value = mock_result
    mark_for_deployment.wait_for_deployment('service', 'deploy_group_1', 'somesha', '/nail/soa', 1)
