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
import datetime

import pytest
from mock import MagicMock
from mock import patch

from paasta_tools.cli.cmds.get_image_version import get_latest_image_version
from paasta_tools.cli.cmds.get_image_version import paasta_get_image_version
from paasta_tools.utils import DeploymentsJsonV2Dict


@pytest.mark.parametrize(
    "nocommits_enabled, force, mock_latest_version, expected_out, expected_err_match",
    (
        (False, False, None, "", "Automated redeploys not enabled for test_service"),
        (
            False,
            False,
            "00000000T000000",
            "",
            "Automated redeploys not enabled for test_service",
        ),  # disabling nocommits after having been enabled
        (True, False, None, "20220101T000000", ""),
        (True, False, "20210101T000000", "20220101T000000", ""),
        (True, False, "20211231T233000", "20211231T233000", ""),
        (True, True, "20211231T233000", "20220101T000000", ""),
    ),
)
def test_get_image_version(
    capfd,
    nocommits_enabled,
    force,
    mock_latest_version,
    expected_out,
    expected_err_match,
):
    mock_args = MagicMock(
        service="test_service",
        soa_dir="",
        max_age=3600,
        force=force,
        commit="abcdabcd",
    )
    with patch(
        "paasta_tools.cli.cmds.get_image_version.check_enable_automated_redeploys",
        return_value=nocommits_enabled,
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.get_image_version.get_latest_image_version",
        return_value=mock_latest_version,
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.get_image_version.validate_service_name",
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.get_image_version.load_v2_deployments_json",
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.get_image_version.datetime",
        autospec=True,
    ) as mock_datetime:
        mock_datetime.datetime.now.return_value = datetime.datetime(
            year=2022, month=1, day=1
        )
        paasta_get_image_version(mock_args)
        out, err = capfd.readouterr()
        assert expected_out == out.strip()
        assert expected_err_match in err


def test_get_latest_image_version():
    mock_deployments = {
        "a": {
            "docker_image": "image",
            "git_sha": "abcabc",
            "image_version": None,
        },
        "b": {
            "docker_image": "image",
            "git_sha": "abcabc",
            "image_version": "20220101T000000",
        },
        "c": {
            "docker_image": "image",
            "git_sha": "abcabc",
            "image_version": "20220102T000000",
        },
    }

    mock_deployments_json = DeploymentsJsonV2Dict(deployments=mock_deployments)

    latest = get_latest_image_version(
        deployments=mock_deployments_json, commit="abcabc"
    )
    assert latest == "20220102T000000"

    mock_deployments["c"]["git_sha"] = "xyzxyz"

    latest = get_latest_image_version(
        deployments=mock_deployments_json, commit="abcabc"
    )
    assert latest == "20220101T000000"
