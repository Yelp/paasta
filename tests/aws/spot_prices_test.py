# Copyright 2019 Yelp Inc.
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
import datetime

import arrow
import mock
import pytest

import clusterman.aws.spot_prices as spot_prices
from clusterman.aws.client import MAX_PAGE_SIZE


@pytest.fixture
def mock_get_data():
    with mock.patch('clusterman.aws.spot_prices.get_data_for_price', autospec=True) as mock_get:
        yield mock_get


@pytest.fixture()
def mock_generate_key():
    with mock.patch('clusterman.aws.spot_prices.generate_key_with_dimensions', autospec=True) as mock_gen:
        mock_gen.return_value = 'whatever'
        yield mock_gen


def mock_price_object(instance_type, az, time, price):
    return {
        'InstanceType': instance_type,
        'AvailabilityZone': az,
        'Timestamp': time.replace(tzinfo=datetime.timezone.utc),
        'SpotPrice': str(price),
        'ProductDescription': 'Linux/UNIX (Amazon VPC)',
    }


def test_spot_price_generator():
    start_time = arrow.get(2017, 1, 3, 12, 0, 20)
    end_time = arrow.get(2017, 1, 3, 14, 3, 20)
    with mock.patch('clusterman.aws.spot_prices.ec2') as mock_client:
        mock_client.get_paginator.return_value.paginate.return_value = iter([
            {'SpotPriceHistory': [
                mock_price_object('m3.xlarge', 'fake-az-2b', datetime.datetime(2017, 1, 3, 12, 2, 22), '0.1'),
                mock_price_object('c3.xlarge', 'fake-az-2a', datetime.datetime(2017, 1, 3, 12, 1, 40), '0.8'),
            ]},
            {'SpotPriceHistory': [
                mock_price_object('m3.xlarge', 'fake-az-2b', datetime.datetime(2017, 1, 3, 12, 0, 22), '0.1'),
                # Should ignore this last one
                mock_price_object('c3.xlarge', 'fake-az-2a', datetime.datetime(2017, 1, 3, 12, 0, 4), '0.8'),
            ]},
        ])
        prices = spot_prices.spot_price_generator(start_time, end_time)
        result_list = list(prices)

    # Last price was before the start time, so ignore it.
    assert result_list == [
        mock_price_object('m3.xlarge', 'fake-az-2b', datetime.datetime(2017, 1, 3, 12, 2, 22), '0.1'),
        mock_price_object('c3.xlarge', 'fake-az-2a', datetime.datetime(2017, 1, 3, 12, 1, 40), '0.8'),
        mock_price_object('m3.xlarge', 'fake-az-2b', datetime.datetime(2017, 1, 3, 12, 0, 22), '0.1'),
    ]

    # Check EC2 query parameters.
    assert mock_client.get_paginator.call_args_list == [mock.call('describe_spot_price_history')]
    assert mock_client.get_paginator.return_value.paginate.call_count == 1
    assert mock_client.get_paginator.return_value.paginate.call_args_list == [mock.call(
        StartTime=start_time.astimezone(tz=datetime.timezone.utc),
        EndTime=end_time.astimezone(tz=datetime.timezone.utc),
        ProductDescriptions=['Linux/UNIX (Amazon VPC)'],
        PaginationConfig={'PageSize': MAX_PAGE_SIZE},
    )]


def test_write_prices_with_dedupe_normal(mock_get_data):
    mock_prices = [
        mock_price_object('m3.xlarge', 'fake-az-2b', datetime.datetime(2017, 1, 3, 12, 2, 22), '0.1'),
        mock_price_object('c3.xlarge', 'fake-az-2a', datetime.datetime(2017, 1, 3, 12, 1, 40), '0.8'),
        mock_price_object('c3.xlarge', 'fake-az-2b', datetime.datetime(2017, 1, 3, 12, 1, 10), '0.4'),
    ]
    mock_writer = mock.MagicMock()
    spot_prices.write_prices_with_dedupe(iter(mock_prices), mock_writer, 60)
    utc = datetime.timezone.utc
    assert mock_get_data.call_args_list == [
        mock.call(mock_prices[0], datetime.datetime(2017, 1, 3, 12, 3, tzinfo=utc)),
        mock.call(mock_prices[1], datetime.datetime(2017, 1, 3, 12, 2, tzinfo=utc)),
        mock.call(mock_prices[2], datetime.datetime(2017, 1, 3, 12, 2, tzinfo=utc)),
    ]
    assert mock_writer.send.call_args_list == [mock.call(mock_get_data.return_value) for _ in range(3)]


def test_write_prices_with_dedupe_multiple(mock_get_data):
    mock_prices = [
        # Several in one minute for one market
        mock_price_object('m3.xlarge', 'fake-az-2b', datetime.datetime(2017, 1, 3, 12, 5, 39), '0.3'),
        mock_price_object('m3.xlarge', 'fake-az-2b', datetime.datetime(2017, 1, 3, 12, 5, 22), '0.2'),
        mock_price_object('m3.xlarge', 'fake-az-2b', datetime.datetime(2017, 1, 3, 12, 5, 10), '0.1'),
        # Other market in that minute
        mock_price_object('m3.large', 'fake-az-2b', datetime.datetime(2017, 1, 3, 12, 5, 10), '0.7'),
    ]
    mock_writer = mock.MagicMock()
    spot_prices.write_prices_with_dedupe(iter(mock_prices), mock_writer, 60)
    utc = datetime.timezone.utc
    assert mock_get_data.call_args_list == [
        mock.call(mock_prices[0], datetime.datetime(2017, 1, 3, 12, 6, tzinfo=utc)),
        mock.call(mock_prices[3], datetime.datetime(2017, 1, 3, 12, 6, tzinfo=utc)),
    ]
    assert mock_writer.send.call_args_list == [mock.call(mock_get_data.return_value) for _ in range(2)]


def test_write_prices_with_dedupe_boundary(mock_get_data):
    mock_prices = [
        # Several in nearby minutes for one market
        mock_price_object('m3.xlarge', 'fake-az-2b', datetime.datetime(2017, 1, 3, 12, 15, 39), '0.3'),
        mock_price_object('m3.xlarge', 'fake-az-2b', datetime.datetime(2017, 1, 3, 12, 15, 0), '0.2'),
        mock_price_object('m3.xlarge', 'fake-az-2b', datetime.datetime(2017, 1, 3, 12, 14, 39), '0.1'),
    ]
    mock_writer = mock.MagicMock()
    spot_prices.write_prices_with_dedupe(iter(mock_prices), mock_writer, 60)
    utc = datetime.timezone.utc
    assert mock_get_data.call_args_list == [
        mock.call(mock_prices[0], datetime.datetime(2017, 1, 3, 12, 16, tzinfo=utc)),
        mock.call(mock_prices[1], datetime.datetime(2017, 1, 3, 12, 15, tzinfo=utc)),
    ]
    assert mock_writer.send.call_args_list == [mock.call(mock_get_data.return_value) for _ in range(2)]


def test_get_data_for_price(mock_generate_key):
    instance_type = 'm3.xlarge'
    az = 'fake-az-1a'
    real_time = datetime.datetime(2017, 3, 12, 3, 4, 23, tzinfo=datetime.timezone.utc)
    rounded_up_time = arrow.get(2017, 3, 12, 3, 5)
    price = '0.23'
    price_object = mock_price_object(instance_type, az, real_time, price)
    _, timestamp, value = spot_prices.get_data_for_price(price_object, rounded_up_time)

    expected_dimensions = {
        'aws_instance_type': instance_type,
        'aws_availability_zone': az,
    }
    # Should use whatever time is passed in, not the price object's time.
    assert timestamp == int(rounded_up_time.timestamp)
    assert value == price
    assert mock_generate_key.call_args_list == [mock.call('spot_prices', dimensions=expected_dimensions)]
