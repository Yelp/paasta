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

from paasta_tools.cli.cmds.autoscale import paasta_autoscale


@mock.patch("paasta_tools.cli.cmds.autoscale.figure_out_service_name", autospec=True)
@mock.patch("paasta_tools.cli.cmds.autoscale.client.get_paasta_api_client", autospec=True)
@mock.patch("paasta_tools.cli.cmds.autoscale._log_audit", autospec=True)
def test_paasta_autoscale(mock__log_audit, mock_get_paasta_api_client, mock_figure_out_service_name):
    service = 'fake_service'
    instance = 'fake_instance'
    cluster = 'fake_cluster'

    mock_figure_out_service_name.return_value = service
    mock_get_paasta_api_client.return_value = mock.MagicMock()

    args = mock.MagicMock()
    args.service = service
    args.clusters = cluster
    args.instances = instance
    args.set = 14

    fake_result = mock.MagicMock()
    fake_result.result.return_value = {"desired_instances": 14}, mock.Mock()
    mock_get_paasta_api_client.return_value.autoscaler.update_autoscaler_count.return_value = fake_result
    mock__log_audit.return_value = None

    paasta_autoscale(args)
    assert mock_get_paasta_api_client.return_value.autoscaler.update_autoscaler_count.call_count == 1
