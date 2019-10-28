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
import mock
import pytest

from clusterman.exceptions import PoolManagerError
from clusterman.mesos.util import agent_pid_to_ip
from clusterman.mesos.util import allocated_agent_resources
from clusterman.mesos.util import mesos_post


@pytest.fixture
def mock_market_capacities():
    return {'market-1': 1000, 'market-2': 5}


@pytest.fixture
def mock_agent_pid_to_ip():
    with mock.patch('clusterman.mesos.util.agent_pid_to_ip') as mock_agent_pid_to_ip:
        mock_agent_pid_to_ip.return_value = '1.2.3.4'
        yield


def test_agent_pid_to_ip():
    ret = agent_pid_to_ip('slave(1)@10.40.31.172:5051')
    assert ret == '10.40.31.172'


def test_allocated_agent_resources(mock_agents_response):
    assert allocated_agent_resources(mock_agents_response.json()['slaves'][0])[0] == 0
    assert allocated_agent_resources(mock_agents_response.json()['slaves'][1])[0] == 0
    assert allocated_agent_resources(mock_agents_response.json()['slaves'][2])[0] == 10
    assert allocated_agent_resources(mock_agents_response.json()['slaves'][2])[1] == 20


@mock.patch('clusterman.mesos.util.mesos_post', wraps=mesos_post)
class TestMesosPost:
    def test_success(self, wrapped_post):
        with mock.patch('clusterman.mesos.util.requests'):
            wrapped_post('http://the.mesos.master/', 'an-endpoint')
        assert wrapped_post.call_count == 2
        assert wrapped_post.call_args_list == [
            mock.call('http://the.mesos.master/', 'an-endpoint'),
            mock.call('http://the.mesos.master/', 'redirect'),
        ]

    def test_failure(self, wrapped_post):
        with mock.patch('clusterman.mesos.util.requests') as mock_requests, \
                pytest.raises(PoolManagerError):
            mock_requests.post.side_effect = Exception('something bad happened')
            wrapped_post('http://the.mesos.master/', 'an-endpoint')
