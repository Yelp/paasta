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

from mock import Mock
from mock import patch

from paasta_tools.paasta_cli.cmds.mark_for_deployment import get_loglines
from paasta_tools.paasta_cli.cmds.mark_for_deployment import paasta_mark_for_deployment


class fake_args:
    clusterinstance = 'cluster.instance'
    service = 'test_service'
    git_url = 'git://false.repo/services/test_services'
    commit = 'fake-hash'


@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment._run', autospec=True)
@patch('sys.exit', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment._log', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment.validate_service_name', autospec=True)
def test_mark_for_deployment_run_fail(
    mock_validate_service_name,
    mock_log,
    mock_exit,
    mock_run,
):
    mock_run.return_value = (1, 'Exterminate!')
    paasta_mark_for_deployment(fake_args)
    mock_exit.assert_called_once_with(1)


@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment._run', autospec=True)
@patch('sys.exit', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment._log', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment.validate_service_name', autospec=True)
def test_mark_for_deployment_success(
    mock_validate_service_name,
    mock_log,
    mock_exit,
    mock_run,
):
    mock_run.return_value = (0, 'Interminate!')
    assert paasta_mark_for_deployment(fake_args) is None


def test_get_loglines_good_hides_output():
    returncode = 0
    cmd = 'testcmd'
    output = 'goodoutput'
    args = Mock()
    args.commit = 'testcommit'
    args.clusterinstance = 'test-clusterinstance'
    actual = get_loglines(returncode=returncode, cmd=cmd, output=output, args=args)
    assert 'Marked %s in %s for deployment.' % (args.commit, args.clusterinstance) in actual
    assert 'Output: %s' % output not in actual


def test_get_loglines_bad_return_outputs_the_error():
    returncode = 1
    cmd = 'testcmd'
    output = 'BAD OUTPUT'
    args = Mock()
    args.commit = 'testcommit'
    args.clusterinstance = 'test-clusterinstance'
    actual = get_loglines(returncode=returncode, cmd=cmd, output=output, args=args)
    assert 'Output: %s' % output in actual
    assert "Ran: '%s'" % cmd in actual
    assert 'ERROR: Failed to mark %s for deployment in %s.' % (args.commit, args.clusterinstance) in actual
