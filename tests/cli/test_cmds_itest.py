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

from paasta_tools.cli.cmds.itest import paasta_itest


@patch("paasta_tools.cli.cmds.itest.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.itest._run", autospec=True)
@patch("paasta_tools.cli.cmds.itest._log", autospec=True)
@patch("paasta_tools.cli.cmds.itest.check_docker_image", autospec=True)
@patch("paasta_tools.cli.cmds.itest.build_docker_tag", autospec=True)
def test_itest_run_fail(
    mock_build_docker_tag,
    mock_docker_image,
    mock_log,
    mock_run,
    mock_validate_service_name,
):
    mock_build_docker_tag.return_value = "fake-registry/services-foo:paasta-bar"
    mock_docker_image.return_value = True
    mock_run.return_value = (1, "fake_output")
    args = MagicMock()
    assert paasta_itest(args) == 1


@patch("paasta_tools.cli.cmds.itest.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.itest._run", autospec=True)
@patch("paasta_tools.cli.cmds.itest._log", autospec=True)
@patch("paasta_tools.cli.cmds.itest.check_docker_image", autospec=True)
@patch("paasta_tools.cli.cmds.itest.build_docker_tag", autospec=True)
def test_itest_success(
    mock_build_docker_tag,
    mock_docker_image,
    mock_log,
    mock_run,
    mock_validate_service_name,
):
    mock_build_docker_tag.return_value = "fake-registry/services-foo:paasta-bar"
    mock_docker_image.return_value = True
    mock_run.return_value = (0, "Yeeehaaa")
    args = MagicMock()
    assert paasta_itest(args) == 0


@patch("paasta_tools.cli.cmds.itest.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.itest._run", autospec=True)
@patch("paasta_tools.cli.cmds.itest.build_docker_tag", autospec=True)
@patch("paasta_tools.cli.cmds.itest._log", autospec=True)
@patch("paasta_tools.cli.cmds.itest.check_docker_image", autospec=True)
def test_itest_works_when_service_name_starts_with_services_dash(
    mock_docker_image,
    mock_log,
    mock_build_docker_tag,
    mock_run,
    mock_validate_service_name,
):
    mock_docker_image.return_value = True
    mock_build_docker_tag.return_value = "unused_docker_tag"
    mock_run.return_value = (0, "Yeeehaaa")
    args = MagicMock()
    args.service = "services-fake_service"
    args.commit = "unused"
    assert paasta_itest(args) == 0
    mock_build_docker_tag.assert_called_once_with("fake_service", "unused")
