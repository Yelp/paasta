#!/usr/bin/env python
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
from mock import Mock
from mock import patch

from paasta_tools.cli.cmds.rollback import get_git_shas_for_service
from paasta_tools.cli.cmds.rollback import list_previously_deployed_shas
from paasta_tools.cli.cmds.rollback import paasta_rollback


@patch('paasta_tools.cli.cmds.rollback.list_deploy_groups', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.get_git_url', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.mark_for_deployment', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.wait_for_deployment', autospec=True)
@patch('paasta_tools.cli.cmds.rollback._log', autospec=True)
def test_paasta_rollback_mark_for_deployment_simple_invocation(
    mock_log,
    mock_wait_for_deployment,
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_list_deploy_groups,
):

    fake_args = Mock(
        deploy_group='fake_deploy_group',
        commit='123456'
    )

    mock_get_git_url.return_value = 'git://git.repo'
    mock_figure_out_service_name.return_value = 'fakeservice'

    mock_list_deploy_groups.return_value = ['fake_deploy_group']

    mock_mark_for_deployment.return_value = 0
    assert paasta_rollback(fake_args) == 0

    mock_mark_for_deployment.assert_called_once_with(
        git_url=mock_get_git_url.return_value,
        deploy_group=fake_args.deploy_group,
        service=mock_figure_out_service_name.return_value,
        commit=fake_args.commit
    )

    assert mock_mark_for_deployment.call_count == 1


@patch('paasta_tools.cli.cmds.rollback.list_deploy_groups', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.get_git_url', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.mark_for_deployment', autospec=True)
def test_paasta_rollback_mark_for_deployment_no_deploy_group_arg(
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_list_deploy_groups,
):

    fake_args = Mock(
        commit='123456',
        deploy_group=None,
    )

    mock_get_git_url.return_value = 'git://git.repo'
    mock_figure_out_service_name.return_value = 'fakeservice'

    mock_list_deploy_groups.return_value = [
        'fake_deploy_group', 'fake_cluster.fake_instance']

    mock_mark_for_deployment.return_value = 0
    assert paasta_rollback(fake_args) == 1


@patch('paasta_tools.cli.cmds.rollback.list_deploy_groups', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.get_git_url', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.mark_for_deployment', autospec=True)
def test_paasta_rollback_mark_for_deployment_wrong_deploy_group_args(
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_list_deploy_groups,
):

    fake_args = Mock(
        commit='123456',
        deploy_group='test_group,fake_deploy.group',
    )

    mock_get_git_url.return_value = 'git://git.repo'
    mock_figure_out_service_name.return_value = 'fakeservice'

    mock_list_deploy_groups.return_value = ['some_other_instance.some_other_cluster']

    assert paasta_rollback(fake_args) == 1
    assert mock_mark_for_deployment.call_count == 0


def test_list_previously_deployed_shas():
    fake_refs = {
        'refs/tags/paasta-test.deploy.group-00000000T000000-deploy': 'SHA_IN_OUTPUT',
        'refs/tags/paasta-other.deploy.group-00000000T000000-deploy': 'NOT_IN_OUTPUT',
    }
    with patch('paasta_tools.cli.cmds.rollback.list_remote_refs', autospec=True, return_value=fake_refs):
        fake_args = Mock(
            service='fake_service',
            deploy_group='test.deploy.group',
            soa_dir='/fake/soa/dir',
        )
        assert set(list_previously_deployed_shas(fake_args)) == {'SHA_IN_OUTPUT'}


def test_get_git_shas_for_service_no_service_name():
    assert get_git_shas_for_service(None, None, '/fake/soa/dir') == []
