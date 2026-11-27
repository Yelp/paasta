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
import os
import re
import tempfile
from time import time

from behave import given
from behave import then
from behave import when
from dulwich.objects import Blob
from dulwich.objects import Commit
from dulwich.objects import Tree
from dulwich.objects import parse_timezone
from dulwich.repo import Repo

from paasta_tools.utils import _run

SERVICE = "fake_deploy_history_service"

# A table row: "<tstamp> (<human time>)  <deploy group>  <version>".
# Columns are separated by 2+ spaces; single spaces only appear inside a column.
TABLE_ROW = re.compile(
    r"^(?P<tstamp>\d{8}T\d{6}) \([^)]*\)\s{2,}(?P<deploy_group>\S+)\s{2,}(?P<version>.+)$"
)

# Lightweight tags in the same format mark-for-deployment pushes, pointing at
# one of two commits ("first" / "second").
DEPLOY_TAGS = {
    "paasta-test-cluster.main-20260101T000000-deploy": "first",
    "paasta-test-cluster.canary-20260201T000000-deploy": "second",
    "paasta-test-cluster.main-20260301T000000-deploy": "second",
    # "first" is redeployed: deploy-history must show it again
    "paasta-test-cluster.main-20260401T000000-deploy": "first",
    # rollback tags are not deploys and must be ignored
    "paasta-test-cluster.main-20260501T000000-rollback": "first",
}


def _make_commit(repo: Repo, content: bytes, message: bytes, parents) -> bytes:
    blob = Blob.from_string(content)
    tree = Tree()
    tree.add(b"spam", 0o0100644, blob.id)

    commit = Commit()
    commit.author = commit.committer = b"itest author <itest@example.com>"
    commit.commit_time = commit.author_time = int(time())
    commit.commit_timezone = commit.author_timezone = parse_timezone(b"-0200")[0]
    commit.message = message
    commit.tree = tree.id
    commit.parents = parents

    repo.object_store.add_object(blob)
    repo.object_store.add_object(tree)
    repo.object_store.add_object(commit)
    return commit.id


@given("a service with a git repo that has deploy tags")
def step_service_with_deploy_tags(context):
    # cleaned up by after_scenario in environment.py
    context.tmpdir = tempfile.mkdtemp("paasta_tools_deploy_history_itest")

    git_repo_dir = os.path.join(context.tmpdir, "repo")
    os.makedirs(git_repo_dir)
    repo = Repo.init(git_repo_dir)
    first = _make_commit(repo, b"first\n", b"First commit", [])
    second = _make_commit(repo, b"second\n", b"Second commit", [first])
    repo.refs[b"refs/heads/master"] = second
    context.commits = {"first": first.decode(), "second": second.decode()}
    for tag, commit_name in DEPLOY_TAGS.items():
        repo.refs[f"refs/tags/{tag}".encode()] = context.commits[commit_name].encode()

    # soa-configs for the service, pointing its git_url at the local repo
    # (clusters come from fake_etc_paasta/clusters.json, which has test-cluster)
    context.soa_dir = os.path.join(context.tmpdir, "soa_configs")
    service_dir = os.path.join(context.soa_dir, SERVICE)
    os.makedirs(service_dir)
    with open(os.path.join(service_dir, "service.yaml"), "w") as f:
        f.write(f"git_url: {git_repo_dir}\n")
    with open(os.path.join(service_dir, "eks-test-cluster.yaml"), "w") as f:
        f.write(
            "main:\n"
            "  cpus: 0.1\n"
            "canary:\n"
            "  cpus: 0.1\n"
            "  deploy_group: test-cluster.canary\n"
        )


@when("we run paasta deploy-history")
def step_run_deploy_history_no_args(context):
    step_run_deploy_history(context, "")


@when('we run paasta deploy-history with "{extra_args}"')
def step_run_deploy_history(context, extra_args):
    cmd = f"paasta deploy-history -s {SERVICE} -d {context.soa_dir} {extra_args}"
    context.return_code, context.output = _run(command=cmd)
    print(context.output)


@then("the history should list, newest first")
def step_history_matches(context):
    expected = [
        (row["timestamp"], row["deploy_group"], context.commits[row["commit"]])
        for row in context.table
    ]
    # only table rows match, so the header, warnings and footer are skipped
    matches = (TABLE_ROW.match(line) for line in context.output.splitlines())
    actual = [(m["tstamp"], m["deploy_group"], m["version"]) for m in matches if m]
    assert actual == expected, f"actual: {actual}\nexpected: {expected}"


@then("the output should not contain the rollback tag")
def step_no_rollback_tag(context):
    assert "20260501T000000" not in context.output
