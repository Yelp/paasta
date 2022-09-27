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

from paasta_tools import deployment_utils
from paasta_tools.utils import DeploymentsJsonV2
from paasta_tools.utils import DeploymentVersion


@mock.patch("paasta_tools.deployment_utils.load_v2_deployments_json", autospec=True)
def test_get_currently_deployed_sha(
    mock_load_v2_deployments_json,
):
    mock_load_v2_deployments_json.return_value = DeploymentsJsonV2(
        service="fake-service",
        config_dict={
            "controls": {},
            "deployments": {"everything": {"git_sha": "abc", "docker_image": "foo"}},
        },
    )
    actual = deployment_utils.get_currently_deployed_sha(
        service="service", deploy_group="everything"
    )
    assert actual == "abc"


@mock.patch("paasta_tools.deployment_utils.load_v2_deployments_json", autospec=True)
def test_get_currently_deployed_version_no_image(
    mock_load_v2_deployments_json,
):
    mock_load_v2_deployments_json.return_value = DeploymentsJsonV2(
        service="fake-service",
        config_dict={
            "controls": {},
            "deployments": {"everything": {"git_sha": "abc", "docker_image": "foo"}},
        },
    )
    actual = deployment_utils.get_currently_deployed_version(
        service="service", deploy_group="everything"
    )
    assert actual == DeploymentVersion(sha="abc", image_version=None)


@mock.patch("paasta_tools.deployment_utils.load_v2_deployments_json", autospec=True)
def test_get_currently_deployed_version(
    mock_load_v2_deployments_json,
):
    mock_load_v2_deployments_json.return_value = DeploymentsJsonV2(
        service="fake-service",
        config_dict={
            "controls": {},
            "deployments": {
                "everything": {
                    "git_sha": "abc",
                    "docker_image": "foo",
                    "image_version": "extrastuff",
                }
            },
        },
    )
    actual = deployment_utils.get_currently_deployed_version(
        service="service", deploy_group="everything"
    )
    assert actual == DeploymentVersion(sha="abc", image_version="extrastuff")
