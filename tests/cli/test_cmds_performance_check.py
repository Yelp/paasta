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
from pytest import raises

from paasta_tools.cli.cmds import performance_check


@mock.patch(
    "paasta_tools.cli.cmds.performance_check.validate_service_name", autospec=True
)
@mock.patch("requests.post", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.performance_check.load_performance_check_config",
    autospec=True,
)
def test_submit_performance_check_job_happy(
    mock_load_performance_check_config, mock_requests_post, mock_validate_service_name
):
    fake_endpoint = "http://foo:1234/submit"
    mock_load_performance_check_config.return_value = {
        "endpoint": fake_endpoint,
        "fake_param": "fake_value",
    }
    mock_validate_service_name.return_value = True
    performance_check.submit_performance_check_job("fake_service", "fake_soa_dir")
    mock_requests_post.assert_called_once_with(
        url=fake_endpoint, params={"fake_param": "fake_value"}
    )


@mock.patch(
    "paasta_tools.cli.cmds.performance_check.validate_service_name", autospec=True
)
@mock.patch(
    "paasta_tools.cli.cmds.performance_check.submit_performance_check_job",
    autospec=True,
)
def test_main_safely_returns_when_exceptions(
    mock_submit_performance_check_job, mock_validate_service_name
):
    mock_validate_service_name.return_value = True
    fake_args = mock.Mock()
    fake_args.service = "services-fake_service"
    fake_args.soa_dir = "fake_soa_dir"
    mock_submit_performance_check_job.side_effect = raises(Exception)
    performance_check.perform_performance_check(fake_args)
    mock_submit_performance_check_job.assert_called_once_with(
        service="fake_service", soa_dir="fake_soa_dir"
    )
