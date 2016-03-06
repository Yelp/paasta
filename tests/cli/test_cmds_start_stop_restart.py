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
import contextlib
from StringIO import StringIO

import mock

from paasta_tools import remote_git
from paasta_tools.cli.cmds import start_stop_restart
from paasta_tools.marathon_tools import MarathonServiceConfig


def test_format_tag():
    expected = 'refs/tags/paasta-BRANCHNAME-TIMESTAMP-stop'
    actual = start_stop_restart.format_tag(
        branch='BRANCHNAME',
        force_bounce='TIMESTAMP',
        desired_state='stop'
    )
    assert actual == expected


@mock.patch('paasta_tools.utils.get_git_url', autospec=True)
@mock.patch('dulwich.client.get_transport_and_path', autospec=True)
@mock.patch('paasta_tools.cli.cmds.start_stop_restart.log_event', autospec=True)
def test_issue_state_change_for_service(mock_log_event, get_transport_and_path, get_git_url):
    fake_git_url = 'BLOORGRGRGRGR'
    fake_path = 'somepath'

    get_git_url.return_value = fake_git_url

    mock_git_client = mock.Mock()
    get_transport_and_path.return_value = (mock_git_client, fake_path)

    start_stop_restart.issue_state_change_for_service(
        MarathonServiceConfig(
            cluster='fake_cluster',
            instance='fake_instance',
            service='fake_service',
            config_dict={},
            branch_dict={},
        ),
        '0',
        'stop'
    )

    get_transport_and_path.assert_called_once_with(fake_git_url)
    mock_git_client.send_pack.assert_called_once_with(fake_path, mock.ANY,
                                                      mock.ANY)
    assert mock_log_event.call_count == 1


def test_make_mutate_refs_func():

    mutate_refs = start_stop_restart.make_mutate_refs_func(
        service_config=MarathonServiceConfig(
            cluster='fake_cluster',
            instance='fake_instance',
            service='fake_service',
            config_dict={'deploy_group': 'a'},
            branch_dict={},
        ),
        force_bounce='FORCEBOUNCE',
        desired_state='stop',
    )

    old_refs = {
        'refs/heads/paasta-a': 'hash_for_a',
        'refs/heads/paasta-b': 'hash_for_b',
        'refs/heads/paasta-c': 'hash_for_c',
        'refs/heads/paasta-d': 'hash_for_d',
    }

    expected = dict(old_refs)
    expected.update({
        'refs/tags/paasta-fake_cluster.fake_instance-FORCEBOUNCE-stop': 'hash_for_a',
    })

    actual = mutate_refs(old_refs)
    assert actual == expected


def test_log_event():
    with contextlib.nested(
        mock.patch('paasta_tools.utils.get_username', autospec=True, return_value='fake_user'),
        mock.patch('socket.getfqdn', autospec=True, return_value='fake_fqdn'),
        mock.patch('paasta_tools.utils._log', autospec=True),
    ) as (
        mock_get_username,
        mock_getfqdn,
        mock_log,
    ):
        service_config = MarathonServiceConfig(
            cluster='fake_cluster',
            instance='fake_instance',
            service='fake_service',
            config_dict={'deploy_group': 'fake_deploy_group'},
            branch_dict={},
        )
        start_stop_restart.log_event(service_config, 'stopped')
        mock_log.assert_called_once_with(
            instance='fake_instance',
            service='fake_service',
            level='event',
            component='deploy',
            cluster='fake_cluster',
            line="Issued request to change state of fake_instance to 'stopped' by fake_user@fake_fqdn"
        )


@mock.patch('sys.stdout', new_callable=StringIO)
@mock.patch('paasta_tools.cli.cmds.start_stop_restart.remote_git.list_remote_refs', autospec=True)
@mock.patch('paasta_tools.cli.cmds.start_stop_restart.figure_out_service_name', autospec=True)
@mock.patch('paasta_tools.cli.cmds.start_stop_restart.get_instance_config', autospec=True)
@mock.patch('paasta_tools.cli.cmds.start_stop_restart.utils.get_git_url', autospec=True)
def test_stop_or_start_handls_ls_remote_failures(
    mock_get_git_url,
    mock_get_instance_config,
    mock_figure_out_service_name,
    mock_list_remote_refs,
    mock_stdout,
):
    args = mock.Mock()
    mock_get_git_url.return_value = 'fake_git_url'
    mock_figure_out_service_name.return_value = 'fake_service'
    mock_get_instance_config.return_value = None
    mock_list_remote_refs.side_effect = remote_git.LSRemoteException

    assert start_stop_restart.paasta_start_or_stop(args, 'restart') == 1
    assert "may be down" in mock_stdout.getvalue()
