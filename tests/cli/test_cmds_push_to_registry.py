# Copyright 2015 Yelp Inc.
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

from paasta_tools.cli.cmds.push_to_registry import build_command
from paasta_tools.cli.cmds.push_to_registry import paasta_push_to_registry


@patch('paasta_tools.cli.cmds.push_to_registry.build_docker_tag')
def test_build_command(mock_build_docker_tag):
    mock_build_docker_tag.return_value = 'my-docker-registry/services-foo:paasta-asdf'
    expected = 'docker push my-docker-registry/services-foo:paasta-asdf'
    actual = build_command('foo', 'bar')
    assert actual == expected


@patch('paasta_tools.cli.cmds.push_to_registry.build_command')
@patch('paasta_tools.cli.cmds.push_to_registry.validate_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.push_to_registry._run', autospec=True)
@patch('paasta_tools.cli.cmds.push_to_registry._log', autospec=True)
def test_push_to_registry_run_fail(
    mock_log,
    mock_run,
    mock_validate_service_name,
    mock_build_command,
):
    mock_build_command.return_value = 'docker push my-docker-registry/services-foo:paasta-asdf'
    mock_run.return_value = (1, 'Bad')
    args = MagicMock()
    assert paasta_push_to_registry(args) == 1


@patch('paasta_tools.cli.cmds.push_to_registry.build_command')
@patch('paasta_tools.cli.cmds.push_to_registry.validate_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.push_to_registry._run', autospec=True)
@patch('paasta_tools.cli.cmds.push_to_registry._log', autospec=True)
def test_push_to_registry_success(
    mock_log,
    mock_run,
    mock_validate_service_name,
    mock_build_command,
):
    mock_build_command.return_value = 'docker push my-docker-registry/services-foo:paasta-asdf'
    mock_run.return_value = (0, 'Success')
    args = MagicMock()
    assert paasta_push_to_registry(args) == 0


@patch('paasta_tools.cli.cmds.push_to_registry.validate_service_name', autospec=True)
@patch('paasta_tools.cli.cmds.push_to_registry._run', autospec=True)
@patch('paasta_tools.cli.cmds.push_to_registry._log', autospec=True)
@patch('paasta_tools.cli.cmds.push_to_registry.build_command', autospec=True)
def test_push_to_registry_works_when_service_name_starts_with_services_dash(
    mock_build_command,
    mock_log,
    mock_run,
    mock_validate_service_name,
):
    mock_run.return_value = (0, 'Success')
    args = MagicMock()
    args.service = 'fake_service'
    args.commit = 'unused'
    assert paasta_push_to_registry(args) == 0
    mock_build_command.assert_called_once_with('fake_service', 'unused')
