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
import contextlib

from mock import call
from mock import Mock
from mock import patch

from paasta_tools.cli.cmds.rollback import get_git_shas_for_service
from paasta_tools.cli.cmds.rollback import list_previously_deployed_shas
from paasta_tools.cli.cmds.rollback import paasta_rollback
from paasta_tools.cli.cmds.rollback import validate_given_deploy_groups
from paasta_tools.marathon_tools import MarathonServiceConfig


@patch('paasta_tools.cli.cmds.rollback.get_instance_config_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.get_git_url', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.mark_for_deployment', autospec=True)
def test_paasta_rollback_mark_for_deployment_simple_invocation(
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_get_instance_config,
):

    fake_args = Mock(
        deploy_groups='fake_deploy_groups',
        commit='123456'
    )

    mock_get_git_url.return_value = 'git://git.repo'
    mock_figure_out_service_name.return_value = 'fakeservice'

    mock_get_instance_config.return_value = [MarathonServiceConfig(
        service='',
        cluster='',
        instance='',
        branch_dict={},
        config_dict={'deploy_group': 'fake_deploy_groups'},
    )]

    mock_mark_for_deployment.return_value = 0
    assert paasta_rollback(fake_args) == 0

    mock_mark_for_deployment.assert_called_once_with(
        git_url=mock_get_git_url.return_value,
        deploy_group=fake_args.deploy_groups,
        service=mock_figure_out_service_name.return_value,
        commit=fake_args.commit
    )

    assert mock_mark_for_deployment.call_count == 1


@patch('paasta_tools.cli.cmds.rollback.get_instance_config_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.get_git_url', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.mark_for_deployment', autospec=True)
def test_paasta_rollback_mark_for_deployment_no_deploy_group_arg(
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_get_instance_config,
):

    fake_args = Mock(
        commit='123456',
        deploy_groups='',
    )

    mock_get_git_url.return_value = 'git://git.repo'
    mock_figure_out_service_name.return_value = 'fakeservice'

    mock_get_instance_config.return_value = [
        MarathonServiceConfig(
            service=mock_figure_out_service_name.return_value,
            cluster='',
            instance='',
            config_dict={'deploy_group': 'fake_deploy_group'},
            branch_dict={},
        ),
        MarathonServiceConfig(
            service=mock_figure_out_service_name.return_value,
            cluster='fake_cluster',
            instance='fake_instance',
            config_dict={},
            branch_dict={},
        ),
    ]

    mock_mark_for_deployment.return_value = 0
    assert paasta_rollback(fake_args) == 0

    expected = [
        call(
            git_url=mock_get_git_url.return_value,
            service=mock_figure_out_service_name.return_value,
            commit=fake_args.commit,
            deploy_group='fake_cluster.fake_instance',
        ),
        call(
            git_url=mock_get_git_url.return_value,
            service=mock_figure_out_service_name.return_value,
            commit=fake_args.commit,
            deploy_group='fake_deploy_group',
        ),
    ]

    assert all([x in expected for x in mock_mark_for_deployment.mock_calls])
    assert len(expected) == len(mock_mark_for_deployment.mock_calls)
    assert mock_mark_for_deployment.call_count == 2


@patch('paasta_tools.cli.cmds.rollback.get_instance_config_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.get_git_url', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.mark_for_deployment', autospec=True)
def test_paasta_rollback_mark_for_deployment_wrong_deploy_group_args(
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_get_instance_config_for_service,
):

    fake_args = Mock(
        commit='123456',
        deploy_groups='test_group,fake_deploy.group',
    )

    mock_get_git_url.return_value = 'git://git.repo'
    mock_figure_out_service_name.return_value = 'fakeservice'

    mock_get_instance_config_for_service.return_value = [
        MarathonServiceConfig(
            instance='some_other_instance',
            cluster='some_other_cluster',
            service=mock_figure_out_service_name.return_value,
            config_dict={},
            branch_dict={},
        ),
    ]

    assert paasta_rollback(fake_args) == 1
    assert mock_mark_for_deployment.call_count == 0


@patch('paasta_tools.cli.cmds.rollback.get_instance_config_for_service', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.get_git_url', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.mark_for_deployment', autospec=True)
def test_paasta_rollback_mark_for_deployment_multiple_instance_args(
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_get_instance_config_for_service,
):

    fake_args = Mock(
        deploy_groups='cluster.instance1,cluster.instance2',
        commit='123456',
    )

    mock_get_git_url.return_value = 'git://git.repo'
    mock_figure_out_service_name.return_value = 'fakeservice'

    mock_get_instance_config_for_service.return_value = [
        MarathonServiceConfig(
            instance='instance1',
            cluster='cluster',
            service=mock_figure_out_service_name.return_value,
            config_dict={},
            branch_dict={},
        ),
        MarathonServiceConfig(
            instance='instance2',
            cluster='cluster',
            service=mock_figure_out_service_name.return_value,
            config_dict={},
            branch_dict={},
        ),
    ]

    mock_mark_for_deployment.return_value = 0
    assert paasta_rollback(fake_args) == 0

    expected = [
        call(
            git_url=mock_get_git_url.return_value,
            service=mock_figure_out_service_name.return_value,
            commit=fake_args.commit,
            deploy_group='cluster.instance1',
        ),
        call(
            git_url=mock_get_git_url.return_value,
            service=mock_figure_out_service_name.return_value,
            commit=fake_args.commit,
            deploy_group='cluster.instance2',
        ),
    ]

    mock_mark_for_deployment.assert_has_calls(expected, any_order=True)
    assert mock_mark_for_deployment.call_count == 2


def test_validate_given_deploy_groups_no_arg():
    service_deploy_groups = ['deploy_group1', 'deploy_group2']
    given_deploy_groups = []

    expected_valid = set(['deploy_group1', 'deploy_group2'])
    expected_invalid = set([])

    actual_valid, actual_invalid = validate_given_deploy_groups(service_deploy_groups, given_deploy_groups)

    assert actual_valid == expected_valid
    assert actual_invalid == expected_invalid


def test_validate_given_deploy_groups_wrong_arg():
    service_deploy_groups = ['deploy_group1', 'deploy_group2']
    given_deploy_groups = ['deploy_group0', 'not_an_deploy_group']

    expected_valid = set([])
    expected_invalid = set(['deploy_group0', 'not_an_deploy_group'])

    actual_valid, actual_invalid = validate_given_deploy_groups(service_deploy_groups, given_deploy_groups)

    assert actual_valid == expected_valid
    assert actual_invalid == expected_invalid


def test_validate_given_deploy_groups_single_arg():
    service_deploy_groups = ['deploy_group1', 'deploy_group2']
    given_deploy_groups = ['deploy_group1']

    expected_valid = set(['deploy_group1'])
    expected_invalid = set([])

    actual_valid, actual_invalid = validate_given_deploy_groups(service_deploy_groups, given_deploy_groups)

    assert actual_valid == expected_valid
    assert actual_invalid == expected_invalid


def test_validate_given_deploy_groups_multiple_args():
    service_deploy_groups = ['deploy_group1', 'deploy_group2', 'deploy_group3']
    given_deploy_groups = ['deploy_group1', 'deploy_group2']

    expected_valid = set(['deploy_group1', 'deploy_group2'])
    expected_invalid = set([])

    actual_valid, actual_invalid = validate_given_deploy_groups(service_deploy_groups, given_deploy_groups)

    assert actual_valid == expected_valid
    assert actual_invalid == expected_invalid


def test_validate_given_deploy_groups_duplicate_args():
    service_deploy_groups = ['deploy_group1', 'deploy_group2', 'deploy_group3']
    given_deploy_groups = ['deploy_group1', 'deploy_group1']

    expected_valid = set(['deploy_group1'])
    expected_invalid = set([])

    actual_valid, actual_invalid = validate_given_deploy_groups(service_deploy_groups, given_deploy_groups)

    assert actual_valid == expected_valid
    assert actual_invalid == expected_invalid


def test_list_previously_deployed_shas():
    fake_refs = {
        'refs/tags/paasta-test.deploy.group-00000000T000000-deploy': 'SHA_IN_OUTPUT',
        'refs/tags/paasta-other.deploy.group-00000000T000000-deploy': 'NOT_IN_OUTPUT',
    }
    fake_configs = [MarathonServiceConfig(
        service='fake_service',
        instance='fake_instance',
        cluster='fake_cluster',
        config_dict={'deploy_group': 'test.deploy.group'},
        branch_dict={},
    )]
    with contextlib.nested(
            patch('paasta_tools.cli.cmds.rollback.list_remote_refs', autospec=True, return_value=fake_refs),
            patch('paasta_tools.cli.cmds.rollback.get_instance_config_for_service', autospec=True,
                  return_value=fake_configs),
    ) as (
        _,
        _,
    ):
        fake_args = Mock(
            service='fake_service',
            deploy_groups='test.deploy.group,nonexistant.deploy.group',
        )
        assert set(list_previously_deployed_shas(fake_args)) == set(['SHA_IN_OUTPUT'])


def test_list_previously_deployed_shas_no_deploy_groups():
    fake_refs = {
        'refs/tags/paasta-test.deploy.group-00000000T000000-deploy': 'SHA_IN_OUTPUT',
        'refs/tags/paasta-other.deploy.group-00000000T000000-deploy': 'SHA_IN_OUTPUT_2',
        'refs/tags/paasta-nonexistant.deploy.group-00000000T000000-deploy': 'SHA_NOT_IN_OUTPUT',
    }
    fake_configs = [
        MarathonServiceConfig(
            service='fake_service',
            instance='fake_instance',
            cluster='fake_cluster',
            config_dict={'deploy_group': 'test.deploy.group'},
            branch_dict={},
        ),
        MarathonServiceConfig(
            service='fake_service',
            instance='fake_instance',
            cluster='fake_cluster',
            config_dict={'deploy_group': 'other.deploy.group'},
            branch_dict={},
        ),
    ]
    with contextlib.nested(
            patch('paasta_tools.cli.cmds.rollback.list_remote_refs', autospec=True, return_value=fake_refs),
            patch('paasta_tools.cli.cmds.rollback.get_instance_config_for_service', autospec=True,
                  return_value=fake_configs),
    ) as (
        _,
        _,
    ):
        fake_args = Mock(
            service='fake_service',
            deploy_groups='',
        )
        assert set(list_previously_deployed_shas(fake_args)) == set(['SHA_IN_OUTPUT', 'SHA_IN_OUTPUT_2'])


def test_get_git_shas_for_service_no_service_name():
    assert get_git_shas_for_service(None, None) == []
