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
from pyramid import testing

from paasta_tools.api.views import pause_autoscaler


def test_get_service_autoscaler_pause():
    with mock.patch(
        "paasta_tools.utils.KazooClient", autospec=True
    ) as mock_zk, mock.patch(
        "paasta_tools.utils.load_system_paasta_config", autospec=True
    ):
        request = testing.DummyRequest()
        mock_zk_get = mock.Mock(return_value=(b"100", None))
        mock_zk.return_value = mock.Mock(get=mock_zk_get)

        response = pause_autoscaler.get_service_autoscaler_pause(request)
        mock_zk_get.assert_called_once_with("/autoscaling/paused")
        assert response == "100"


def test_update_autoscaler_pause():
    with mock.patch(
        "paasta_tools.utils.KazooClient", autospec=True
    ) as mock_zk, mock.patch(
        "paasta_tools.api.views.pause_autoscaler.time", autospec=True
    ) as mock_time, mock.patch(
        "paasta_tools.utils.load_system_paasta_config", autospec=True
    ):
        request = testing.DummyRequest()
        request.swagger_data = {"json_body": {"minutes": 100}}
        mock_zk_set = mock.Mock()
        mock_zk_ensure = mock.Mock()
        mock_zk.return_value = mock.Mock(set=mock_zk_set, ensure_path=mock_zk_ensure)

        mock_time.time = mock.Mock(return_value=0)

        response = pause_autoscaler.update_service_autoscaler_pause(request)
        assert mock_zk_ensure.call_count == 2
        mock_zk_set.assert_any_call("/autoscaling/paused", b"6000")
        mock_zk_set.assert_any_call("/autoscaling/resumed", b"False")
        assert response is None


def test_delete_autoscaler_pause():
    with mock.patch(
        "paasta_tools.utils.KazooClient", autospec=True
    ) as mock_zk, mock.patch(
        "paasta_tools.api.views.pause_autoscaler.time", autospec=True
    ) as mock_time, mock.patch(
        "paasta_tools.utils.load_system_paasta_config", autospec=True
    ):
        request = testing.DummyRequest()
        mock_zk_del = mock.Mock()
        mock_zk_ensure = mock.Mock()
        mock_zk.return_value = mock.Mock(delete=mock_zk_del, ensure_path=mock_zk_ensure)

        mock_time.time = mock.Mock(return_value=0)

        response = pause_autoscaler.delete_service_autoscaler_pause(request)
        assert mock_zk_ensure.call_count == 2
        mock_zk_del.assert_any_call("/autoscaling/paused")
        mock_zk_del.assert_any_call("/autoscaling/resumed")
        assert response is None
