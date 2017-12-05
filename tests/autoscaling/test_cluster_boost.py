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
            if not mock_values or key not in mock_values:
                raise cluster_boost.NoNodeError

            return (mock_values[key], None)

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

    fake_end_time = 12345.0
    fake_boost_factor = 1.5
    fake_expected_load = 80

    with patch_zk_client({
        base_path + '/end_time': str(fake_end_time).encode('utf-8'),
        base_path + '/factor': str(fake_boost_factor).encode('utf-8'),
        base_path + '/expected_load': str(fake_expected_load).encode('utf-8'),
    }) as mock_zk_client:

        assert cluster_boost.get_boost_values(
            region=fake_region,
            pool=fake_pool,
            zk=mock_zk_client,
        ) == cluster_boost.BoostValues(
            end_time=fake_end_time,
            boost_factor=fake_boost_factor,
            expected_load=fake_expected_load,
        )


def test_get_boost_values_when_no_values_exist():
    fake_region = 'westeros-1'
    fake_pool = 'default'
    with patch_zk_client() as mock_zk_client:

        assert cluster_boost.get_boost_values(
            region=fake_region,
            pool=fake_pool,
            zk=mock_zk_client,
        ) == cluster_boost.BoostValues(
            end_time=0,
            boost_factor=1.0,
            expected_load=0,
        )


@freeze_time(TEST_CURRENT_TIME)
def test_set_boost_factor_with_defaults():
    fake_region = 'westeros-1'
    fake_pool = 'default'
    base_path = cluster_boost.get_zk_boost_path(fake_region, fake_pool)

    with patch_zk_client() as mock_zk_client:
        cluster_boost.set_boost_factor(fake_region, fake_pool)

    expected_end_time = float(TEST_CURRENT_TIME.timestamp()) + 60 * cluster_boost.DEFAULT_BOOST_DURATION

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


@freeze_time(TEST_CURRENT_TIME)
def test_set_boost_factor_with_active_boost():
    fake_region = 'westeros-1'
    fake_pool = 'default'
    base_path = cluster_boost.get_zk_boost_path(fake_region, fake_pool)

    fake_end_time = float(TEST_CURRENT_TIME.timestamp()) + 10
    fake_boost_factor = 1.5
    fake_expected_load = 80

    # patch zk client so that it returns an end time that
    # indicates an active boost
    with patch_zk_client({
        base_path + '/end_time': str(fake_end_time).encode('utf-8'),
        base_path + '/factor': str(fake_boost_factor).encode('utf-8'),
        base_path + '/expected_load': str(fake_expected_load).encode('utf-8'),
    }):
        # by default, set boost should not go through if there's an active boost
        assert not cluster_boost.set_boost_factor(region=fake_region, pool=fake_pool)


@freeze_time(TEST_CURRENT_TIME)
def test_set_boost_factor_with_active_boost_override():
    fake_region = 'westeros-1'
    fake_pool = 'default'
    base_path = cluster_boost.get_zk_boost_path(fake_region, fake_pool)

    fake_end_time = float(TEST_CURRENT_TIME.timestamp()) + 10
    fake_boost_factor = 1.5
    fake_expected_load = 80

    mock_boost_values = {
        base_path + '/end_time': str(fake_end_time).encode('utf-8'),
        base_path + '/factor': str(fake_boost_factor).encode('utf-8'),
        base_path + '/expected_load': str(fake_expected_load).encode('utf-8'),
    }

    # patch zk client so that it returns an end time that
    # indicates an active boost
    with patch_zk_client(mock_boost_values) as mock_zk_client:

        # we need the zk.set to actually override the initial mocked values
        def mock_set(key, value):
            mock_boost_values[key] = value

        mock_zk_client.set = mock_set

        # set boost will go through with an active boost if override is toggled on
        assert cluster_boost.set_boost_factor(
            region=fake_region,
            pool=fake_pool,
            override=True,
        )


def test_clear_boost():
    pass
