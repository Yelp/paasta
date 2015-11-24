#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
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

from pytest import raises
from mock import patch, Mock
from paasta_tools.paasta_cli.cmds.rollback import paasta_rollback


@patch('paasta_tools.paasta_cli.cmds.rollback.figure_out_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.rollback.list_clusters', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.rollback.get_git_url', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.rollback.mark_for_deployment', autospec=True)
def test_paasta_rollback_mark_for_deployment_invocation(
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_list_clusters,
    mock_figure_out_service_name,
):

    fake_args = Mock(
        cluster='cluster1',
        instance='instance1',
        commit='123456'
    )

    mock_get_git_url.return_value = 'git://git.repo'
    mock_figure_out_service_name.return_value = 'fakeservice'
    mock_list_clusters.return_value = ['cluster1', 'cluster2']

    with raises(SystemExit) as sys_exit:
        paasta_rollback(fake_args)
        assert sys_exit.value_code == 0

    mock_mark_for_deployment.assert_called_once_with(
        git_url=mock_get_git_url.return_value,
        cluster=fake_args.cluster,
        instance=fake_args.instance,
        service=mock_figure_out_service_name.return_value,
        commit=fake_args.commit
    )


@patch('paasta_tools.paasta_cli.cmds.rollback.figure_out_service_name', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.rollback.list_clusters', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.rollback.get_git_url', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.rollback.mark_for_deployment', autospec=True)
def test_paasta_rollback_mark_for_deployment_wrong_cluster(
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_list_clusters,
    mock_figure_out_service_name,
):

    fake_args = Mock(
        cluster='cluster1',
        instance='instance1',
        commit='123456'
    )

    mock_get_git_url.return_value = 'git://git.repo'
    mock_figure_out_service_name.return_value = 'fakeservice'
    mock_list_clusters.return_value = ['cluster0', 'cluster2']

    with raises(SystemExit) as sys_exit:
        paasta_rollback(fake_args)
        assert sys_exit.value_code == 1
