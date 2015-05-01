import contextlib
import os
import shutil
import tempfile

from behave import when, then, given
from dulwich.repo import Repo
from dulwich.objects import Blob
from dulwich.objects import Tree
from dulwich.objects import Commit
from dulwich.objects import parse_timezone
import mock
from time import time

from paasta_tools import generate_deployments_for_service
from paasta_tools import marathon_tools


@given(u'a test git repo is setup with commits')
def step_impl_given(context):
    context.test_git_repo_dir = tempfile.mkdtemp('paasta_tools_deployments_json_itest')
    context.test_git_repo = Repo.init(context.test_git_repo_dir)
    print "Temp repo in %s" % context.test_git_repo_dir

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

    context.test_git_repo.refs['refs/heads/paasta-test_cluster.test_instance'] = commit.id
    context.expected_commit = commit.id


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
    deployments = marathon_tools.load_deployments_json('fake_deployments_json_service', soa_dir='fake_soa_configs')
    expected_deployments = {
        'fake_deployments_json_service:paasta-test_cluster.test_instance': {
            'force_bounce': None,
            'desired_state': 'start',
            'docker_image': 'services-fake_deployments_json_service:paasta-%s' % context.expected_commit
        }
    }
    assert expected_deployments == deployments, "actual: %s\nexpected:%s" % (deployments, expected_deployments)
    shutil.rmtree(context.test_git_repo_dir)
