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
from mock import MagicMock
from mock import patch

from paasta_tools.cli.cmds import get_latest_deployment
from paasta_tools.utils import DeploymentVersion


def test_get_latest_deployment(capfd):
    mock_args = MagicMock(service="", deploy_group="", soa_dir="")
    with patch(
        "paasta_tools.cli.cmds.get_latest_deployment.get_currently_deployed_version",
        return_value=DeploymentVersion(sha="FAKE_SHA", image_version=None),
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.get_latest_deployment.validate_service_name",
        autospec=True,
    ):
        assert get_latest_deployment.paasta_get_latest_deployment(mock_args) == 0
        assert "FAKE_SHA" in capfd.readouterr()[0]


def test_get_latest_deployment_no_deployment_tag(capfd):
    mock_args = MagicMock(
        service="fake_service", deploy_group="fake_deploy_group", soa_dir=""
    )
    with patch(
        "paasta_tools.cli.cmds.get_latest_deployment.get_currently_deployed_version",
        return_value=None,
        autospec=True,
    ), patch(
        "paasta_tools.cli.cmds.get_latest_deployment.validate_service_name",
        autospec=True,
    ):
        assert get_latest_deployment.paasta_get_latest_deployment(mock_args) == 1
        assert (
            "A deployment could not be found for fake_deploy_group in fake_service"
            in capfd.readouterr()[1]
        )
