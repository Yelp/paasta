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
import mock

from paasta_tools import deployment


@mock.patch("paasta_tools.deployment.load_v2_deployments_json", autospec=True)
def test_get_currently_deployed_sha(mock_load_v2_deployments_json,):
    mock_load_v2_deployments_json.return_value = deployment.DeploymentsJsonV2(
        service="fake-service",
        config_dict={
            "controls": {},
            "deployments": {"everything": {"git_sha": "abc", "docker_image": "foo"}},
        },
    )
    actual = deployment.get_currently_deployed_sha(
        service="service", deploy_group="everything"
    )
    assert actual == "abc"


def test_DeploymentsJson_read():
    file_mock = mock.mock_open()
    fake_dir = "/var/dir_of_fake"
    fake_path = "/var/dir_of_fake/fake_service/deployments.json"
    fake_json = {
        "v1": {
            "no_srv:blaster": {
                "docker_image": "test_rocker:9.9",
                "desired_state": "start",
                "force_bounce": None,
            },
            "dont_care:about": {
                "docker_image": "this:guy",
                "desired_state": "stop",
                "force_bounce": "12345",
            },
        }
    }
    with mock.patch(
        "builtins.open", file_mock, autospec=None
    ) as open_patch, mock.patch(
        "json.load", autospec=True, return_value=fake_json
    ) as json_patch, mock.patch(
        "paasta_tools.deployment.os.path.isfile", autospec=True, return_value=True
    ):
        actual = deployment.load_deployments_json("fake_service", fake_dir)
        open_patch.assert_called_once_with(fake_path)
        json_patch.assert_called_once_with(
            file_mock.return_value.__enter__.return_value
        )
        assert actual == deployment.DeploymentsJsonV1(fake_json["v1"])  # type: ignore
