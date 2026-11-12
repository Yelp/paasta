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
from unittest.mock import patch

import pytest

from paasta_tools.cli.cmds.deploy_history import get_deploy_history
from paasta_tools.cli.cmds.deploy_history import print_deploy_history
from paasta_tools.utils import DeploymentVersion

FAKE_REFS = {
    "refs/heads/master": "master_sha",
    "refs/tags/paasta-prod.main-20260101T000000-deploy": "sha_a",
    "refs/tags/paasta-prod.main-20260301T000000-deploy": "sha_b",
    # same sha redeployed later -> must show up as its own row
    "refs/tags/paasta-prod.main-20260401T000000-deploy": "sha_a",
    # same sha deployed to another group -> must show up as its own row
    "refs/tags/paasta-prod.canary-20260201T000000-deploy": "sha_b",
    # rollback tags are not deploys
    "refs/tags/paasta-prod.main-20260501T000000-rollback": "sha_rollback",
    # image_version tag
    "refs/tags/paasta-prod.main+img123-20260601T000000-deploy": "sha_c",
    # deploy group not asked for
    "refs/tags/paasta-dev.everything-20260701T000000-deploy": "sha_dev",
}


@pytest.fixture
def mock_git():
    with patch(
        "paasta_tools.cli.cmds.deploy_history.get_git_url",
        autospec=True,
        return_value="git://git.repo",
    ), patch(
        "paasta_tools.cli.cmds.deploy_history.list_remote_refs",
        autospec=True,
        return_value=FAKE_REFS,
    ) as mock_list_remote_refs:
        yield mock_list_remote_refs


def test_get_deploy_history_filters_and_keeps_every_deploy(mock_git):
    history = get_deploy_history("fakeservice", {"prod.main"}, "/fake/soa")

    assert history == [
        ("20260601T000000", "prod.main", DeploymentVersion("sha_c", "img123")),
        ("20260401T000000", "prod.main", DeploymentVersion("sha_a", None)),
        ("20260301T000000", "prod.main", DeploymentVersion("sha_b", None)),
        ("20260101T000000", "prod.main", DeploymentVersion("sha_a", None)),
    ]
    mock_git.assert_called_once_with("git://git.repo")


def test_get_deploy_history_same_sha_in_multiple_groups(mock_git):
    history = get_deploy_history("fakeservice", {"prod.main", "prod.canary"}, "/x")

    sha_b_rows = [(ts, dg) for ts, dg, v in history if v.sha == "sha_b"]
    assert sha_b_rows == [
        ("20260301T000000", "prod.main"),
        ("20260201T000000", "prod.canary"),
    ]
    assert "sha_rollback" not in {v.sha for _, _, v in history}
    assert "master_sha" not in {v.sha for _, _, v in history}
    assert "sha_dev" not in {v.sha for _, _, v in history}


def _history(n):
    return [
        (f"2026010{i}T000000", "prod.main", DeploymentVersion(f"sha{i}", None))
        for i in range(n, 0, -1)
    ]


def test_print_deploy_history_limit_zero_shows_all(capsys):
    print_deploy_history(_history(5), limit=0)
    out = capsys.readouterr().out

    for i in range(1, 6):
        assert f"sha{i}" in out
    assert "Showing the" not in out


def test_print_deploy_history_image_version_repr(capsys):
    print_deploy_history(
        [("20260601T000000", "prod.main", DeploymentVersion("sha_c", "img123"))],
        limit=10,
    )
    out = capsys.readouterr().out
    assert "DeploymentVersion(sha=sha_c, image_version=img123)" in out


def test_print_deploy_history_empty(capsys):
    print_deploy_history([], limit=10)
    assert "No deploys found" in capsys.readouterr().out
