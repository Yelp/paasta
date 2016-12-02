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
from __future__ import absolute_import
from __future__ import unicode_literals

from StringIO import StringIO

from mock import ANY
from mock import MagicMock
from mock import patch
from pytest import raises

from paasta_tools.cli.cmds.generate_pipeline import generate_pipeline
from paasta_tools.cli.cmds.generate_pipeline import get_git_repo_for_fab_repo
from paasta_tools.cli.cmds.generate_pipeline import paasta_generate_pipeline
from paasta_tools.cli.cmds.generate_pipeline import validate_git_url_for_fab_repo
from paasta_tools.cli.utils import NoSuchService


@patch('paasta_tools.cli.cmds.generate_pipeline.validate_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.generate_pipeline.guess_service_name', autospec=True)
@patch('sys.stdout', new_callable=StringIO, autospec=None)
def test_paasta_generate_pipeline_service_not_found(
        mock_stdout, mock_guess_service_name, mock_validate_service_name):
    # paasta generate cannot guess service name and none is provided

    mock_guess_service_name.return_value = 'not_a_service'
    mock_validate_service_name.side_effect = NoSuchService(None)

    args = MagicMock()
    args.service = None
    expected_output = "%s\n" % NoSuchService.GUESS_ERROR_MSG

    assert paasta_generate_pipeline(args) == 1
    output = mock_stdout.getvalue().decode('utf-8')
    assert output == expected_output


@patch('paasta_tools.cli.cmds.generate_pipeline.get_team_email_address', autospec=True)
@patch('paasta_tools.cli.cmds.generate_pipeline._run', autospec=True)
@patch('paasta_tools.cli.cmds.generate_pipeline.print_warning', autospec=True)
def test_generate_pipeline_run_fails(
        mock_print_warning,
        mock_run,
        mock_get_team_email_address,
):
    mock_get_team_email_address.return_value = 'fake_email'
    mock_run.return_value = (1, 'Big bad wolf')
    assert generate_pipeline('fake_service', '/fake/soa/dir') == 1


@patch('paasta_tools.cli.cmds.generate_pipeline.get_team_email_address', autospec=True)
@patch('paasta_tools.cli.cmds.generate_pipeline._run', autospec=True)
@patch('paasta_tools.cli.cmds.generate_pipeline.print_warning', autospec=True)
def test_generate_pipeline_success(
        mock_print_warning,
        mock_run,
        mock_get_team_email_address,
):
    mock_get_team_email_address.return_value = 'fake_email'
    mock_run.return_value = (0, 'Everything OK')
    assert generate_pipeline('fake_service', '/fake/soa/dir') is None


@patch('paasta_tools.cli.cmds.generate_pipeline._run', autospec=True)
@patch('paasta_tools.cli.cmds.generate_pipeline.get_team_email_address', autospec=True)
@patch('paasta_tools.cli.cmds.generate_pipeline.get_git_repo_for_fab_repo', autospec=True)
@patch('paasta_tools.cli.cmds.generate_pipeline.print_warning', autospec=True)
def test_generate_pipeline_calls_the_right_commands_and_owner(
        mock_print_warning,
        mock_get_git_repo_for_fab_repo,
        mock_get_team_email_address,
        mock_run,
):
    mock_run.return_value = (0, 'Everything OK')
    mock_get_team_email_address.return_value = 'fake_email@yelp.com'
    mock_get_git_repo_for_fab_repo.return_value = 'fake_repo'
    generate_pipeline('fake_service', '/fake/soa/dir')
    assert mock_run.call_count == 2
    expected_cmd1 = ('fab_repo setup_jenkins:services/fake_service,profile=paasta_boilerplate,'
                     'owner=fake_email,repo=fake_repo')
    mock_run.assert_any_call(expected_cmd1, timeout=90)
    expected_cmd2 = ('fab_repo setup_jenkins:services/fake_service,profile=paasta,'
                     'job_disabled=False,owner=fake_email,repo=fake_repo')
    mock_run.assert_any_call(expected_cmd2, timeout=90)


@patch('paasta_tools.cli.cmds.generate_pipeline._run', autospec=True)
@patch('paasta_tools.cli.cmds.generate_pipeline.get_team_email_address', autospec=True)
@patch('paasta_tools.cli.cmds.generate_pipeline.get_team', autospec=True)
@patch('paasta_tools.cli.cmds.generate_pipeline.get_git_repo_for_fab_repo', autospec=True)
@patch('paasta_tools.cli.cmds.generate_pipeline.print_warning', autospec=True)
def test_generate_pipeline_uses_team_name_as_fallback_for_owner(
        mock_print_warning,
        mock_get_git_repo_for_fab_repo,
        mock_get_team,
        mock_get_team_email_address,
        mock_run,
):
    mock_run.return_value = (0, 'Everything OK')
    mock_get_team_email_address.return_value = None
    mock_get_team.return_value = "fake_team"
    mock_get_git_repo_for_fab_repo.return_value = 'fake_repo'
    generate_pipeline('fake_service', '/fake/soa/dir')
    assert mock_run.call_count == 2
    expected_cmd1 = ('fab_repo setup_jenkins:services/fake_service,profile=paasta_boilerplate,'
                     'owner=fake_team,repo=fake_repo')
    mock_run.assert_any_call(expected_cmd1, timeout=90)
    expected_cmd2 = ('fab_repo setup_jenkins:services/fake_service,profile=paasta,'
                     'job_disabled=False,owner=fake_team,repo=fake_repo')
    mock_run.assert_any_call(expected_cmd2, timeout=90)


@patch('paasta_tools.cli.cmds.generate_pipeline.validate_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.generate_pipeline.guess_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.generate_pipeline.generate_pipeline', autospec=True)
def test_paasta_generate_pipeline_success_guesses_service_name(
        mock_generate_pipeline,
        mock_guess_service_name,
        mock_validate_service_name):
    # paasta generate succeeds when service name must be guessed
    mock_guess_service_name.return_value = 'fake_service'
    mock_validate_service_name.return_value = None
    args = MagicMock()
    args.service = None
    assert paasta_generate_pipeline(args) is None
    mock_generate_pipeline.assert_called_once_with(service='fake_service', soa_dir=ANY)


@patch('paasta_tools.cli.cmds.generate_pipeline.validate_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.generate_pipeline.generate_pipeline', autospec=True)
def test_generate_pipeline_success_with_no_opts(
        mock_generate_pipeline,
        mock_validate_service_name):
    # paasta generate succeeds when service name provided as arg
    mock_validate_service_name.return_value = None
    args = MagicMock()
    args.service = 'fake_service'
    assert paasta_generate_pipeline(args) is None
    mock_generate_pipeline.assert_called_once_with(service='fake_service', soa_dir=ANY)


@patch('paasta_tools.cli.cmds.generate_pipeline.get_git_url', autospec=True)
def test_get_git_repo_for_fab_repo_returns_after_colon(mock_get_git_url):
    mock_get_git_url.return_value = 'git@git.yelpcorp.com:fake_service'
    actual = get_git_repo_for_fab_repo('unused', '/fake/soa/dir')
    assert actual == 'fake_service'


@patch('paasta_tools.cli.cmds.generate_pipeline.get_git_url', autospec=True)
def test_get_git_repo_for_fab_repo_handles_services(mock_get_git_url):
    mock_get_git_url.return_value = 'git@git.yelpcorp.com:services/fake_service'
    actual = get_git_repo_for_fab_repo('unused', '/fake/soa/dir')
    assert actual == 'services/fake_service'


def test_validate_git_url_for_fab_repo_happy():
    good_git_url = 'git@git.yelpcorp.com:foobar'
    actual = validate_git_url_for_fab_repo(good_git_url)
    assert actual is True


def test_validate_git_url_for_fab_repo_invalid():
    bad_git_url = 'git@github.com:foobar'
    with raises(NotImplementedError) as exc:
        validate_git_url_for_fab_repo(bad_git_url)
        assert 'cannot handle' in exc.value
        assert bad_git_url in exc.value
