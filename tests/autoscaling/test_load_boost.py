# Copyright 2015-2017 Yelp Inc.
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
from contextlib import contextmanager

import mock

from paasta_tools.autoscaling import load_boost


@contextmanager
def patch_zk_client(mock_values=None):
    with mock.patch(
        "paasta_tools.utils.KazooClient", autospec=True
    ) as mock_client, mock.patch(
        "paasta_tools.utils.load_system_paasta_config", autospec=True
    ):

        def mock_get(key):
            if not mock_values or key not in mock_values:
                raise load_boost.NoNodeError

            return (mock_values[key], None)

        mock_client.return_value = mock.Mock(get=mock_get)
        yield mock_client()


def test_get_zk_cluster_boost_path():
    fake_region = "westeros-1"
    fake_pool = "default"
    expected_result = "/paasta_cluster_autoscaler/westeros-1/default/boost"
    assert (
        load_boost.get_zk_cluster_boost_path(fake_region, fake_pool) == expected_result
    )


def test_get_boost_values():
    fake_region = "westeros-1"
    fake_pool = "default"
    base_path = load_boost.get_zk_cluster_boost_path(fake_region, fake_pool)

    fake_end_time = 12345.0
    fake_boost_factor = 1.5
    fake_expected_load = 80

    with patch_zk_client(
        {
            base_path + "/end_time": str(fake_end_time).encode("utf-8"),
            base_path + "/factor": str(fake_boost_factor).encode("utf-8"),
            base_path + "/expected_load": str(fake_expected_load).encode("utf-8"),
        }
    ) as mock_zk_client:

        assert load_boost.get_boost_values(
            zk_boost_path=f"/paasta_cluster_autoscaler/{fake_region}/{fake_pool}/boost",
            zk=mock_zk_client,
        ) == load_boost.BoostValues(
            end_time=fake_end_time,
            boost_factor=fake_boost_factor,
            expected_load=fake_expected_load,
        )


def test_get_boost_values_when_no_values_exist():
    fake_region = "westeros-1"
    fake_pool = "default"
    with patch_zk_client() as mock_zk_client:

        assert load_boost.get_boost_values(
            zk_boost_path=f"/paasta_cluster_autoscaler/{fake_region}/{fake_pool}/boost",
            zk=mock_zk_client,
        ) == load_boost.BoostValues(end_time=0, boost_factor=1.0, expected_load=0)
