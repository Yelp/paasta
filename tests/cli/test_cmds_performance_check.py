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
import mock
from pytest import raises

from paasta_tools.cli.cmds import performance_check


@mock.patch('requests.post', autospec=True)
@mock.patch('paasta_tools.cli.cmds.performance_check.load_performance_check_config', autospec=True)
@mock.patch('paasta_tools.cli.cmds.performance_check.get_username', autospec=True)
def test_submit_performance_check_job_happy(
    mock_get_username,
    mock_load_performance_check_config,
    mock_requests_post,
):
    fake_endpoint = 'http://foo:1234/submit'
    mock_load_performance_check_config.return_value = {'endpoint': fake_endpoint}
    mock_get_username.return_value = 'fake_user'
    performance_check.submit_performance_check_job('fake_service', 'fake_commit', 'fake_image')
    mock_requests_post.assert_called_once_with(
        url=fake_endpoint,
        data={'submitter': 'fake_user',
              'commit': 'fake_commit',
              'service': 'fake_service',
              'image': 'fake_image'}
    )


@mock.patch('paasta_tools.cli.cmds.performance_check.submit_performance_check_job', autospec=True)
def test_main_safely_returns_when_exceptions(
    mock_submit_performance_check_job,
):
    fake_args = mock.Mock()
    fake_args.service = 'fake_service'
    fake_args.commit = 'fake_commit'
    fake_args.image = 'fake_image'
    mock_submit_performance_check_job.side_effect = raises(Exception)
    performance_check.perform_performance_check(fake_args)
    mock_submit_performance_check_job.assert_called_once_with(
        service='fake_service', commit='fake_commit', image='fake_image'
    )
