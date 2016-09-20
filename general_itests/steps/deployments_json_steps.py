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
import contextlib
import os
import tempfile
from datetime import datetime
from time import time

import mock
from behave import given
from behave import then
from behave import when
from dulwich.objects import Blob
from dulwich.objects import Commit
from dulwich.objects import parse_timezone
from dulwich.objects import Tree
from dulwich.repo import Repo

from paasta_tools import generate_deployments_for_service
from paasta_tools.cli.cmds.mark_for_deployment import paasta_mark_for_deployment
from paasta_tools.cli.cmds.start_stop_restart import paasta_stop
from paasta_tools.utils import format_tag
from paasta_tools.utils import format_timestamp
from paasta_tools.utils import get_paasta_branch_from_deploy_group
from paasta_tools.utils import get_paasta_tag_from_deploy_group
from paasta_tools.utils import load_deployments_json


@given(u'a test git repo is setup with commits')
def step_impl_given(context):
    context.test_git_repo_dir = tempfile.mkdtemp('paasta_tools_deployments_json_itest')
    context.test_git_repo = Repo.init(context.test_git_repo_dir)
    print 'Temp repo in %s' % context.test_git_repo_dir

    blob = Blob.from_string("My file content\n")
    tree = Tree()
    tree.add("spam", 0100644, blob.id)

    commit = Commit()
    commit.author = commit.committer = "itest author"
    commit.commit_time = commit.author_time = int(time())
    commit.commit_timezone = commit.author_timezone = parse_timezone('-0200')[0]
    commit.message = "Initial commit"
    commit.tree = tree.id

    object_store = context.test_git_repo.object_store
    object_store.add_object(blob)
    object_store.add_object(tree)
    object_store.add_object(commit)

    context.test_git_repo.refs['refs/heads/master'] = commit.id
    context.expected_commit = commit.id


@when(u'paasta mark-for-deployments is run against the repo')
def step_paasta_mark_for_deployments_when(context):
    fake_args = mock.MagicMock(
        deploy_group='test_cluster.test_instance',
        service='fake_deployments_json_service',
        git_url=context.test_git_repo_dir,
        commit=context.expected_commit,
        block=False
    )
    context.force_bounce_timestamp = format_timestamp(datetime.utcnow())
    with contextlib.nested(
        mock.patch('paasta_tools.utils.format_timestamp', autosepc=True,
                   return_value=context.force_bounce_timestamp),
        mock.patch('paasta_tools.cli.cmds.mark_for_deployment.validate_service_name', autospec=True,
                   return_value=True),
    ) as (
        mock_format_timestamp,
        mock_validate_service_name,
    ):
        try:
            paasta_mark_for_deployment(fake_args)
        except SystemExit:
            pass


@when(u'paasta stop is run against the repo')
def step_paasta_stop_when(context):
    fake_args = mock.MagicMock(
        clusters='test_cluster',
        instances='test_instance',
        soa_dir='fake_soa_configs',
        service='fake_deployments_json_service',
    )
    context.force_bounce_timestamp = format_timestamp(datetime.utcnow())
    with contextlib.nested(
        mock.patch('paasta_tools.cli.cmds.start_stop_restart.utils.get_git_url', autospec=True,
                   return_value=context.test_git_repo_dir),
        mock.patch('paasta_tools.utils.format_timestamp', autospec=True,
                   return_value=context.force_bounce_timestamp),
    ) as (
        mock_get_git_url,
        mock_get_timestamp,
    ):
        try:
            paasta_stop(fake_args)
        except SystemExit:
            pass


@when(u'we generate deployments.json for that service')
def step_impl_when(context):
    context.deployments_file = os.path.join('fake_soa_configs', 'fake_deployments_json_service', 'deployments.json')
    try:
        os.remove(context.deployments_file)
    except OSError:
        pass
    fake_args = mock.MagicMock(
        service='fake_deployments_json_service',
        soa_dir='fake_soa_configs',
        verbose=True,
    )
    with contextlib.nested(
        mock.patch('paasta_tools.generate_deployments_for_service.get_git_url', autospec=True,
                   return_value=context.test_git_repo_dir),
        mock.patch('paasta_tools.generate_deployments_for_service.parse_args',
                   autospec=True, return_value=fake_args),
    ) as (
        mock_get_git_url,
        mock_parse_args,
    ):
        generate_deployments_for_service.main()


@then(u'that deployments.json can be read back correctly')
def step_impl_then(context):
    deployments = load_deployments_json('fake_deployments_json_service', soa_dir='fake_soa_configs')
    expected_deployments = {
        'fake_deployments_json_service:paasta-test_cluster.test_instance': {
            'force_bounce': context.force_bounce_timestamp,
            'desired_state': 'stop',
            'docker_image': 'services-fake_deployments_json_service:paasta-%s' % context.expected_commit
        },
        'fake_deployments_json_service:paasta-test_cluster.test_instance_2': {
            'force_bounce': None,
            'desired_state': 'start',
            'docker_image': 'services-fake_deployments_json_service:paasta-%s' % context.expected_commit,
        },
    }
    assert expected_deployments == deployments, "actual: %s\nexpected:%s" % (deployments, expected_deployments)


@then(u'that deployments.json has a desired_state of "{expected_state}"')
def step_impl_then_desired_state(context, expected_state):
    deployments = load_deployments_json('fake_deployments_json_service', soa_dir='fake_soa_configs')
    latest = sorted(deployments.iteritems(), key=lambda(key, value): value['force_bounce'], reverse=True)[0][1]
    desired_state = latest['desired_state']
    assert desired_state == expected_state, "actual: %s\nexpected: %s" % (desired_state, expected_state)


@then(u'the repository should be correctly tagged')
def step_impl_then_correctly_tagged(context):
    with contextlib.nested(
        mock.patch('paasta_tools.utils.format_timestamp', autosepc=True,
                   return_value=context.force_bounce_timestamp),
    ) as (
        mock_format_timestamp,
    ):
        expected_tag = get_paasta_tag_from_deploy_group(identifier='test_cluster.test_instance', desired_state='deploy')
    expected_formatted_tag = format_tag(expected_tag)
    assert expected_formatted_tag in context.test_git_repo.refs
    assert context.test_git_repo.refs[expected_formatted_tag] == context.expected_commit


@then(u'the repository should not have old style branches')
def step_impl_then_no_old_style_branches(context):
    old_style_branch = get_paasta_branch_from_deploy_group(identifier='test_cluster.test_instance')
    formatted_old_style_branch = 'refs/heads/%s' % old_style_branch
    assert formatted_old_style_branch not in context.test_git_repo.refs
