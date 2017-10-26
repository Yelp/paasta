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
from datetime import datetime

import mock
from freezegun import freeze_time

from paasta_tools.autoscaling import cluster_boost

TEST_CURRENT_TIME = datetime(2020, 2, 14)


@contextmanager
def patch_zk_client(mock_values=None):
    with mock.patch(
        'paasta_tools.utils.KazooClient',
        autospec=True,
    ) as mock_client, mock.patch(
        'paasta_tools.utils.load_system_paasta_config', autospec=True,
    ):
        def mock_get(key):
            return (mock_values.get(key, None), None) if mock_values else None

        mock_client.return_value = mock.Mock(get=mock_get)
        yield mock_client()


def test_get_zk_boost_path():
    fake_region = 'westeros-1'
    fake_pool = 'default'
    expected_result = '/paasta_cluster_autoscaler/westeros-1/default/boost'
    assert cluster_boost.get_zk_boost_path(fake_region, fake_pool) == expected_result


def test_get_boost_values():
    fake_region = 'westeros-1'
    fake_pool = 'default'
    base_path = cluster_boost.get_zk_boost_path(fake_region, fake_pool)

    fake_end_time = 12345
    fake_boost_factor = 1.5
    fake_expected_load = 80

    with patch_zk_client({
        base_path + '/end_time': str(fake_end_time).encode('utf-8'),
        base_path + '/factor': str(fake_boost_factor).encode('utf-8'),
        base_path + '/expected_load': str(fake_expected_load).encode('utf-8'),
    }) as mock_zk_client:

        assert cluster_boost.get_boost_values(fake_region, fake_pool, mock_zk_client) == (
            fake_end_time,
            fake_boost_factor,
            fake_expected_load,
        )


@freeze_time(TEST_CURRENT_TIME)
def test_set_boost_factor_with_defaults():
    fake_region = 'westeros-1'
    fake_pool = 'default'
    base_path = cluster_boost.get_zk_boost_path(fake_region, fake_pool)

    with patch_zk_client() as mock_zk_client:
        cluster_boost.set_boost_factor(fake_region, fake_pool)

    expected_end_time = int(TEST_CURRENT_TIME.timestamp()) + 60 * cluster_boost.DEFAULT_BOOST_DURATION

    assert mock_zk_client.set.call_args_list == [
        mock.call(
            base_path + '/end_time',
            str(expected_end_time).encode('utf-8'),
        ),
        mock.call(
            base_path + '/factor',
            str(cluster_boost.DEFAULT_BOOST_FACTOR).encode('utf-8'),
        ),
        mock.call(
            base_path + '/expected_load',
            '0'.encode('utf-8'),
        ),
    ]


def test_set_boost_factor():
    pass


def test_set_boost_factor_with_active_boost():
    pass


def test_set_boost_factor_with_active_boost_override():
    pass


def test_clear_boost():
    pass
