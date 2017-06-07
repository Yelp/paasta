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
from __future__ import absolute_import
from __future__ import unicode_literals

from mock import call
from mock import Mock
from mock import patch

from paasta_tools.cli.cli import parse_args
from paasta_tools.cli.cmds.rollback import get_git_shas_for_service
from paasta_tools.cli.cmds.rollback import list_previously_deployed_shas
from paasta_tools.cli.cmds.rollback import paasta_rollback
from paasta_tools.cli.cmds.rollback import validate_given_deploy_groups


@patch('paasta_tools.cli.cmds.rollback.list_deploy_groups', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.get_git_url', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.mark_for_deployment', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.get_git_shas_for_service', autospec=True)
def test_paasta_rollback_mark_for_deployment_simple_invocation(
    mock_get_git_shas_for_service,
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_list_deploy_groups,
):
    fake_args, _ = parse_args(['rollback',
                               '-s', 'fakeservice',
                               '-k', 'abcd' * 10,
                               '-l', 'fake_deploy_group1'])

    mock_get_git_shas_for_service.return_value = {
        fake_args.commit: ('20170403T025512', fake_args.deploy_groups),
        'dcba' * 10: ('20161006T025416', 'fake_deploy_group2')}

    mock_get_git_url.return_value = 'git://git.repo'
    mock_figure_out_service_name.return_value = fake_args.service
    mock_list_deploy_groups.return_value = [fake_args.deploy_groups]
    mock_mark_for_deployment.return_value = 0

    assert paasta_rollback(fake_args) == 0

    mock_mark_for_deployment.assert_called_once_with(
        git_url=mock_get_git_url.return_value,
        deploy_group=fake_args.deploy_groups,
        service=mock_figure_out_service_name.return_value,
        commit=fake_args.commit
    )


@patch('paasta_tools.cli.cmds.rollback.list_deploy_groups', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.get_git_url', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.mark_for_deployment', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.get_git_shas_for_service', autospec=True)
def test_paasta_rollback_with_force(
    mock_get_git_shas_for_service,
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_list_deploy_groups,
):
    fake_args, _ = parse_args(['rollback',
                               '-s', 'fakeservice',
                               '-k', 'abcd' * 10,
                               '-l', 'fake_deploy_group1',
                               '-f'])

    mock_get_git_shas_for_service.return_value = {
        'fake_sha1': ('20170403T025512', 'fake_deploy_group1'),
        'fake_sha2': ('20161006T025416', 'fake_deploy_group2')}

    mock_get_git_url.return_value = 'git://git.repo'
    mock_figure_out_service_name.return_value = fake_args.service
    mock_list_deploy_groups.return_value = [fake_args.deploy_groups]
    mock_mark_for_deployment.return_value = 0

    assert paasta_rollback(fake_args) == 0

    mock_mark_for_deployment.assert_called_once_with(
        git_url=mock_get_git_url.return_value,
        deploy_group=fake_args.deploy_groups,
        service=mock_figure_out_service_name.return_value,
        commit=fake_args.commit
    )


@patch('paasta_tools.cli.cmds.rollback.list_deploy_groups', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.get_git_url', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.mark_for_deployment', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.get_git_shas_for_service', autospec=True)
def test_paasta_rollback_mark_for_deployment_no_deploy_group_arg(
    mock_get_git_shas_for_service,
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_list_deploy_groups,
):
    fake_args, _ = parse_args(['rollback',
                               '-s', 'fakeservice',
                               '-k', 'abcd' * 10])

    mock_get_git_shas_for_service.return_value = {
        'fake_sha1': ('20170403T025512', 'fake_deploy_group1'),
        fake_args.commit: ('20161006T025416', 'fake_deploy_group2')}

    mock_get_git_url.return_value = 'git://git.repo'
    mock_figure_out_service_name.return_value = fake_args.service
    mock_list_deploy_groups.return_value = [
        'fake_deploy_group', 'fake_cluster.fake_instance']
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
    assert mock_mark_for_deployment.call_count == len(expected)


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
    fake_args, _ = parse_args(['rollback',
                               '-s', 'fakeservice',
                               '-k', 'abcd' * 10,
                               '-l', 'wrong_deploy_group'])

    mock_get_git_url.return_value = 'git://git.repo'
    mock_figure_out_service_name.return_value = fake_args.service
    mock_list_deploy_groups.return_value = ['some_other_instance.some_other_cluster']

    assert paasta_rollback(fake_args) == 1
    assert not mock_mark_for_deployment.called


@patch('paasta_tools.cli.cmds.rollback.list_deploy_groups', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.get_git_url', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.mark_for_deployment', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.get_git_shas_for_service', autospec=True)
def test_paasta_rollback_git_sha_was_not_marked_before(
    mock_get_git_shas_for_service,
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_list_deploy_groups,
):
    fake_args, _ = parse_args(['rollback',
                               '-s', 'fakeservice',
                               '-k', 'abcd' * 10,
                               '-l', 'fake_deploy_group1'])

    mock_get_git_shas_for_service.return_value = {
        'fake_sha1': ('20170403T025512', 'fake_deploy_group1'),
        'fake_sha2': ('20161006T025416', 'fake_deploy_group2')}

    mock_get_git_url.return_value = 'git://git.repo'
    mock_figure_out_service_name.return_value = fake_args.service
    mock_list_deploy_groups.return_value = [fake_args.deploy_groups]
    mock_mark_for_deployment.return_value = 0

    assert paasta_rollback(fake_args) == 1
    assert not mock_mark_for_deployment.called


@patch('paasta_tools.cli.cmds.rollback.list_deploy_groups', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.figure_out_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.get_git_url', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.mark_for_deployment', autospec=True)
@patch('paasta_tools.cli.cmds.rollback.get_git_shas_for_service', autospec=True)
def test_paasta_rollback_mark_for_deployment_multiple_deploy_group_args(
    mock_get_git_shas_for_service,
    mock_mark_for_deployment,
    mock_get_git_url,
    mock_figure_out_service_name,
    mock_list_deploy_groups,
):
    fake_args, _ = parse_args(['rollback',
                               '-s', 'fakeservice',
                               '-k', 'abcd' * 10,
                               '-l', 'cluster.instance1,cluster.instance2'])

    fake_deploy_groups = fake_args.deploy_groups.split(',')

    mock_get_git_shas_for_service.return_value = {
        'fake_sha1': ('20170403T025512', 'fake_deploy_group1'),
        fake_args.commit: ('20161006T025416', 'fake_deploy_group2')}

    mock_get_git_url.return_value = 'git://git.repo'
    mock_figure_out_service_name.return_value = fake_args.service
    mock_list_deploy_groups.return_value = fake_deploy_groups
    mock_mark_for_deployment.return_value = 0

    assert paasta_rollback(fake_args) == 0

    expected = [
        call(
            git_url=mock_get_git_url.return_value,
            service=mock_figure_out_service_name.return_value,
            commit=fake_args.commit,
            deploy_group=deploy_group,
        ) for deploy_group in fake_deploy_groups
    ]

    mock_mark_for_deployment.assert_has_calls(expected, any_order=True)
    assert mock_mark_for_deployment.call_count == len(fake_deploy_groups)


def test_validate_given_deploy_groups_no_arg():
    service_deploy_groups = ['deploy_group1', 'deploy_group2']
    given_deploy_groups = []

    expected_valid = {'deploy_group1', 'deploy_group2'}
    expected_invalid = set()

    actual_valid, actual_invalid = validate_given_deploy_groups(service_deploy_groups, given_deploy_groups)

    assert actual_valid == expected_valid
    assert actual_invalid == expected_invalid


def test_validate_given_deploy_groups_wrong_arg():
    service_deploy_groups = ['deploy_group1', 'deploy_group2']
    given_deploy_groups = ['deploy_group0', 'not_an_deploy_group']

    expected_valid = set()
    expected_invalid = {'deploy_group0', 'not_an_deploy_group'}

    actual_valid, actual_invalid = validate_given_deploy_groups(service_deploy_groups, given_deploy_groups)

    assert actual_valid == expected_valid
    assert actual_invalid == expected_invalid


def test_validate_given_deploy_groups_single_arg():
    service_deploy_groups = ['deploy_group1', 'deploy_group2']
    given_deploy_groups = ['deploy_group1']

    expected_valid = {'deploy_group1'}
    expected_invalid = set()

    actual_valid, actual_invalid = validate_given_deploy_groups(service_deploy_groups, given_deploy_groups)

    assert actual_valid == expected_valid
    assert actual_invalid == expected_invalid


def test_validate_given_deploy_groups_multiple_args():
    service_deploy_groups = ['deploy_group1', 'deploy_group2', 'deploy_group3']
    given_deploy_groups = ['deploy_group1', 'deploy_group2']

    expected_valid = {'deploy_group1', 'deploy_group2'}
    expected_invalid = set()

    actual_valid, actual_invalid = validate_given_deploy_groups(service_deploy_groups, given_deploy_groups)

    assert actual_valid == expected_valid
    assert actual_invalid == expected_invalid


def test_validate_given_deploy_groups_duplicate_args():
    service_deploy_groups = ['deploy_group1', 'deploy_group2', 'deploy_group3']
    given_deploy_groups = ['deploy_group1', 'deploy_group1']

    expected_valid = {'deploy_group1'}
    expected_invalid = set()

    actual_valid, actual_invalid = validate_given_deploy_groups(service_deploy_groups, given_deploy_groups)

    assert actual_valid == expected_valid
    assert actual_invalid == expected_invalid


def test_list_previously_deployed_shas():
    fake_refs = {
        'refs/tags/paasta-test.deploy.group-00000000T000000-deploy': 'SHA_IN_OUTPUT',
        'refs/tags/paasta-other.deploy.group-00000000T000000-deploy': 'NOT_IN_OUTPUT',
    }
    fake_deploy_groups = ['test.deploy.group']

    with patch(
        'paasta_tools.cli.cmds.rollback.list_remote_refs', autospec=True, return_value=fake_refs,
    ), patch(
        'paasta_tools.cli.cmds.rollback.list_deploy_groups', autospec=True,
        return_value=fake_deploy_groups,
    ):
        fake_args = Mock(
            service='fake_service',
            deploy_groups='test.deploy.group,nonexistant.deploy.group',
            soa_dir='/fake/soa/dir',
            force=None,
        )
        assert set(list_previously_deployed_shas(fake_args)) == {'SHA_IN_OUTPUT'}


def test_list_previously_deployed_shas_no_deploy_groups():
    fake_refs = {
        'refs/tags/paasta-test.deploy.group-00000000T000000-deploy': 'SHA_IN_OUTPUT',
        'refs/tags/paasta-other.deploy.group-00000000T000000-deploy': 'SHA_IN_OUTPUT_2',
        'refs/tags/paasta-nonexistant.deploy.group-00000000T000000-deploy': 'SHA_NOT_IN_OUTPUT',
    }
    fake_deploy_groups = ['test.deploy.group', 'other.deploy.group']

    with patch(
        'paasta_tools.cli.cmds.rollback.list_remote_refs', autospec=True, return_value=fake_refs,
    ), patch(
        'paasta_tools.cli.cmds.rollback.list_deploy_groups', autospec=True,
        return_value=fake_deploy_groups,
    ):
        fake_args = Mock(
            service='fake_service',
            deploy_groups='',
            soa_dir='/fake/soa/dir',
            force=None,
        )
        assert set(list_previously_deployed_shas(fake_args)) == {'SHA_IN_OUTPUT', 'SHA_IN_OUTPUT_2'}


def test_get_git_shas_for_service_no_service_name():
    assert get_git_shas_for_service(None, None, '/fake/soa/dir') == []
