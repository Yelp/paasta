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

from paasta_tools.cli.cmds.deploy_history import get_deploy_history
from paasta_tools.cli.cmds.deploy_history import print_deploy_history
from paasta_tools.utils import DeploymentVersion


def test_get_deploy_history_filters_and_keeps_every_deploy():
    refs = {
        "refs/heads/master": "master_sha",
        "refs/tags/paasta-prod.main-20260101T000000-deploy": "sha_a",
        "refs/tags/paasta-prod.canary-20260201T000000-deploy": "sha_b",
        "refs/tags/paasta-prod.main-20260301T000000-deploy": "sha_b",
        # same sha redeployed later -> must show up as its own row
        "refs/tags/paasta-prod.main-20260401T000000-deploy": "sha_a",
        # rollback tags are not deploys
        "refs/tags/paasta-prod.main-20260501T000000-rollback": "sha_rollback",
        "refs/tags/paasta-prod.main+img123-20260601T000000-deploy": "sha_c",
        # deploy group not asked for
        "refs/tags/paasta-dev.everything-20260701T000000-deploy": "sha_dev",
    }
    with patch(
        "paasta_tools.cli.cmds.deploy_history.get_git_url",
        autospec=True,
        return_value="git://git.repo",
    ), patch(
        "paasta_tools.cli.cmds.deploy_history.list_remote_refs",
        autospec=True,
        return_value=refs,
    ) as mock_list_remote_refs:
        history = get_deploy_history(
            "fakeservice", {"prod.main", "prod.canary"}, "/fake/soa"
        )

    mock_list_remote_refs.assert_called_once_with("git://git.repo")
    # DeployHistoryEntry equality ignores `version`, so compare it explicitly
    assert [(e.tstamp, e.deploy_group, e.version) for e in history] == [
        ("20260601T000000", "prod.main", DeploymentVersion("sha_c", "img123")),
        ("20260401T000000", "prod.main", DeploymentVersion("sha_a", None)),
        ("20260301T000000", "prod.main", DeploymentVersion("sha_b", None)),
        ("20260201T000000", "prod.canary", DeploymentVersion("sha_b", None)),
        ("20260101T000000", "prod.main", DeploymentVersion("sha_a", None)),
    ]


def test_print_deploy_history_empty(capsys):
    print_deploy_history([], limit=10)
    assert "No deploys found" in capsys.readouterr().out
