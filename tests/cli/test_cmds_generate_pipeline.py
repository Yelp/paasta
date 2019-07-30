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
from mock import ANY
from mock import MagicMock
from mock import patch

from paasta_tools.cli.cmds.generate_pipeline import get_git_repo_for_fab_repo
from paasta_tools.cli.cmds.generate_pipeline import paasta_generate_pipeline
from paasta_tools.cli.utils import NoSuchService


@patch("paasta_tools.cli.cmds.generate_pipeline.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.generate_pipeline.guess_service_name", autospec=True)
def test_paasta_generate_pipeline_service_not_found(
    mock_guess_service_name, mock_validate_service_name, capfd
):
    # paasta generate cannot guess service name and none is provided

    mock_guess_service_name.return_value = "not_a_service"
    mock_validate_service_name.side_effect = NoSuchService(None)

    args = MagicMock()
    args.service = None
    expected_output = "%s\n" % NoSuchService.GUESS_ERROR_MSG

    assert paasta_generate_pipeline(args) == 1
    output, _ = capfd.readouterr()
    assert output == expected_output


@patch("paasta_tools.cli.cmds.generate_pipeline.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.generate_pipeline.guess_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.generate_pipeline.generate_pipeline", autospec=True)
@patch("paasta_tools.cli.cmds.generate_pipeline._log_audit", autospec=True)
def test_paasta_generate_pipeline_success_guesses_service_name(
    mock_log_audit,
    mock_generate_pipeline,
    mock_guess_service_name,
    mock_validate_service_name,
):
    # paasta generate succeeds when service name must be guessed
    mock_guess_service_name.return_value = "fake_service"
    mock_validate_service_name.return_value = None
    args = MagicMock()
    args.service = None
    assert paasta_generate_pipeline(args) is None
    mock_generate_pipeline.assert_called_once_with(service="fake_service", soa_dir=ANY)
    mock_log_audit.assert_called_once_with(
        action="generate-pipeline", service="fake_service"
    )


@patch("paasta_tools.cli.cmds.generate_pipeline.validate_service_name", autospec=True)
@patch("paasta_tools.cli.cmds.generate_pipeline.generate_pipeline", autospec=True)
@patch("paasta_tools.cli.cmds.generate_pipeline._log_audit", autospec=True)
def test_generate_pipeline_success_with_no_opts(
    mock_log_audit, mock_generate_pipeline, mock_validate_service_name
):
    # paasta generate succeeds when service name provided as arg
    mock_validate_service_name.return_value = None
    args = MagicMock()
    args.service = "fake_service"
    assert paasta_generate_pipeline(args) is None
    mock_generate_pipeline.assert_called_once_with(service="fake_service", soa_dir=ANY)
    mock_log_audit.assert_called_once_with(
        action="generate-pipeline", service="fake_service"
    )


@patch("paasta_tools.cli.cmds.generate_pipeline.get_git_url", autospec=True)
def test_get_git_repo_for_fab_repo_returns_after_colon(mock_get_git_url):
    mock_get_git_url.return_value = "git@git.yelpcorp.com:fake_service"
    actual = get_git_repo_for_fab_repo("unused", "/fake/soa/dir")
    assert actual == "fake_service"


@patch("paasta_tools.cli.cmds.generate_pipeline.get_git_url", autospec=True)
def test_get_git_repo_for_fab_repo_handles_services(mock_get_git_url):
    mock_get_git_url.return_value = "git@git.yelpcorp.com:services/fake_service"
    actual = get_git_repo_for_fab_repo("unused", "/fake/soa/dir")
    assert actual == "services/fake_service"
