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

from mock import patch

from paasta_tools.paasta_cli.cmds.mark_for_deployment import get_loglines
from paasta_tools.paasta_cli.cmds.mark_for_deployment import mark_for_deployment


class fake_args:
    cluster = 'cluster'
    instance = 'instance'
    service = 'test_service'
    git_url = 'git://false.repo/services/test_services'
    commit = 'fake-hash'


@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment._run', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment._log', autospec=True)
def test_mark_for_deployment_run_fail(
    mock_log,
    mock_run,
):
    mock_run.return_value = (1, 'Exterminate!')
    actual = mark_for_deployment(
        fake_args.git_url,
        fake_args.cluster,
        fake_args.instance,
        fake_args.service,
        fake_args.commit
    )
    assert actual == 1


@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment._run', autospec=True)
@patch('paasta_tools.paasta_cli.cmds.mark_for_deployment._log', autospec=True)
def test_mark_for_deployment_success(
    mock_log,
    mock_run,
):
    mock_run.return_value = (0, 'Interminate!')
    assert mark_for_deployment(
        fake_args.git_url,
        fake_args.cluster,
        fake_args.instance,
        fake_args.service,
        fake_args.commit
    ) == 0


def test_get_loglines_good_hides_output():
    returncode = 0
    cmd = 'testcmd'
    output = 'goodoutput'
    commit = 'testcommit'
    cluster = 'test-cluster'
    instance = 'test-instance'
    actual = get_loglines(
        returncode=returncode,
        cmd=cmd,
        output=output,
        commit=commit,
        cluster=cluster,
        instance=instance
    )
    assert 'Marked %s in %s.%s for deployment.' % (commit, cluster, instance) in actual
    assert 'Output: %s' % output not in actual


def test_get_loglines_bad_return_outputs_the_error():
    returncode = 1
    cmd = 'testcmd'
    output = 'BAD OUTPUT'
    commit = 'testcommit'
    cluster = 'test-cluster'
    instance = 'test-instance'
    actual = get_loglines(
        returncode=returncode,
        cmd=cmd,
        output=output,
        commit=commit,
        cluster=cluster,
        instance=instance
    )
    assert 'Output: %s' % output in actual
    assert "Ran: '%s'" % cmd in actual
    assert 'ERROR: Failed to mark %s for deployment in %s.%s.' % (commit, cluster, instance) in actual
