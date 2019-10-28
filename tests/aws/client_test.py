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
import math

import mock
import pytest
from mock import call

from clusterman.aws.client import ec2_describe_instances
from clusterman.aws.client import MAX_PAGE_SIZE


def test_empty_instance_ids():
    assert ec2_describe_instances(instance_ids=None) == []
    assert ec2_describe_instances(instance_ids=[]) == []


@pytest.mark.parametrize('value_numbers', [200, 500, 1100])
def test_over_filter_limits(value_numbers):
    instance_ids = list(range(value_numbers))
    with mock.patch('clusterman.aws.client.ec2.describe_instances') as mock_describe_instances:
        ec2_describe_instances(instance_ids)
        target_call_count = math.ceil(value_numbers / MAX_PAGE_SIZE)
        assert mock_describe_instances.call_count == target_call_count
        assert mock_describe_instances.call_args_list == [
            call(InstanceIds=instance_ids[i * MAX_PAGE_SIZE:(i + 1) * MAX_PAGE_SIZE])
            for i in range(target_call_count)
        ]
