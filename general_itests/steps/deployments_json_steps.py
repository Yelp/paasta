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
import json
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
from paasta_tools.utils import DeploymentsJsonV1
from paasta_tools.utils import format_tag
from paasta_tools.utils import format_timestamp
from paasta_tools.utils import get_paasta_tag_from_deploy_group
from paasta_tools.utils import load_deployments_json


@given("a test git repo is setup with commits")
def step_impl_given(context):
    context.test_git_repo_dir = tempfile.mkdtemp("paasta_tools_deployments_json_itest")
    context.test_git_repo = Repo.init(context.test_git_repo_dir)
    print("Temp repo in %s" % context.test_git_repo_dir)

    blob = Blob.from_string(b"My file content\n")
    tree = Tree()
    tree.add(b"spam", 0o0100644, blob.id)

    commit = Commit()
    commit.author = commit.committer = b"itest author"
    commit.commit_time = commit.author_time = int(time())
    commit.commit_timezone = commit.author_timezone = parse_timezone(b"-0200")[0]
    commit.message = b"Initial commit"
    commit.tree = tree.id

    object_store = context.test_git_repo.object_store
    object_store.add_object(blob)
    object_store.add_object(tree)
    object_store.add_object(commit)

    context.test_git_repo.refs[b"refs/heads/master"] = commit.id
    context.expected_commit_as_bytes = commit.id
    context.expected_commit = context.expected_commit_as_bytes.decode()


@given("a valid system paasta config")
def generate_system_paasta_config(context):
    system_paasta_config_dir = os.environ["PAASTA_SYSTEM_CONFIG_DIR"]
    if not os.path.exists(system_paasta_config_dir):
        os.makedirs(system_paasta_config_dir)
    with open("%s/clusters.json" % system_paasta_config_dir, "w+") as f:
        print(
            json.dumps({"clusters": ["test-cluster"]}, sort_keys=True, indent=4), file=f
        )


@when("paasta mark-for-deployments is run against the repo")
def step_paasta_mark_for_deployments_when(context):
    fake_args = mock.MagicMock(
        deploy_group="test-cluster.test_instance",
        service="fake_deployments_json_service",
        git_url=context.test_git_repo_dir,
        commit=context.expected_commit,
        image_version=None,
        soa_dir="fake_soa_configs",
        block=False,
        verify_image=False,
        auto_rollback=False,
        auto_certify_delay=None,
    )
    context.force_bounce_timestamp = format_timestamp(datetime.utcnow())
    with mock.patch(
        "paasta_tools.utils.format_timestamp",
        autospec=True,
        return_value=context.force_bounce_timestamp,
    ), mock.patch(
        "paasta_tools.cli.cmds.mark_for_deployment.validate_service_name",
        autospec=True,
        return_value=True,
    ):
        try:
            paasta_mark_for_deployment(fake_args)
        except SystemExit:
            pass


@when("paasta stop is run against the repo")
def step_paasta_stop_when(context):
    fake_args = mock.MagicMock(
        clusters="test-cluster",
        instances="test_instance",
        soa_dir="fake_soa_configs",
        service="fake_deployments_json_service",
        deploy_group=None,
        verify_image=False,
    )
    context.force_bounce_timestamp = format_timestamp(datetime.utcnow())
    with mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.utils.get_git_url",
        autospec=True,
        return_value=context.test_git_repo_dir,
    ), mock.patch(
        "paasta_tools.utils.format_timestamp",
        autospec=True,
        return_value=context.force_bounce_timestamp,
    ), mock.patch(
        "paasta_tools.cli.cmds.start_stop_restart.apply_args_filters",
        autospec=True,
        return_value={
            fake_args.clusters: {fake_args.service: {fake_args.instances: None}}
        },
    ):
        try:
            paasta_stop(fake_args)
        except SystemExit:
            pass


@when("we generate deployments.json for that service")
def step_impl_when(context):
    context.deployments_file = os.path.join(
        "fake_soa_configs", "fake_deployments_json_service", "deployments.json"
    )
    try:
        os.remove(context.deployments_file)
    except OSError:
        pass
    fake_args = mock.MagicMock(
        service="fake_deployments_json_service",
        soa_dir="fake_soa_configs",
        verbose=True,
    )
    with mock.patch(
        "paasta_tools.generate_deployments_for_service.get_git_url",
        autospec=True,
        return_value=context.test_git_repo_dir,
    ), mock.patch(
        "paasta_tools.generate_deployments_for_service.parse_args",
        autospec=True,
        return_value=fake_args,
    ):
        generate_deployments_for_service.main()


@then("that deployments.json can be read back correctly")
def step_impl_then(context):
    deployments = load_deployments_json(
        "fake_deployments_json_service", soa_dir="fake_soa_configs"
    )
    expected_deployments = DeploymentsJsonV1(
        {
            "fake_deployments_json_service:paasta-test-cluster.test_instance": {
                "force_bounce": context.force_bounce_timestamp,
                "desired_state": "stop",
                "docker_image": "services-fake_deployments_json_service:paasta-%s"
                % context.expected_commit,
            },
            "fake_deployments_json_service:paasta-test-cluster.test_instance_2": {
                "force_bounce": None,
                "desired_state": "start",
                "docker_image": "services-fake_deployments_json_service:paasta-%s"
                % context.expected_commit,
            },
        }
    )
    assert (
        expected_deployments == deployments
    ), f"actual: {deployments}\nexpected:{expected_deployments}"


@then('that deployments.json has a desired_state of "{expected_state}"')
def step_impl_then_desired_state(context, expected_state):
    deployments = load_deployments_json(
        "fake_deployments_json_service", soa_dir="fake_soa_configs"
    )
    latest = sorted(
        deployments.config_dict.items(),
        key=lambda kv: kv[1]["force_bounce"] or "",
        reverse=True,
    )[0][1]
    desired_state = latest["desired_state"]
    assert (
        desired_state == expected_state
    ), f"actual: {desired_state}\nexpected: {expected_state}"


@then("the repository should be correctly tagged")
def step_impl_then_correctly_tagged(context):
    with mock.patch(
        "paasta_tools.utils.format_timestamp",
        autospec=True,
        return_value=context.force_bounce_timestamp,
    ):
        expected_tag = get_paasta_tag_from_deploy_group(
            identifier="test-cluster.test_instance", desired_state="deploy"
        )
    expected_formatted_tag = format_tag(expected_tag).encode("UTF-8")
    assert expected_formatted_tag in context.test_git_repo.refs
    assert (
        context.test_git_repo.refs[expected_formatted_tag]
        == context.expected_commit_as_bytes
    )
