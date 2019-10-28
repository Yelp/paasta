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
import arrow
import mock
import pytest
import staticconf.testing

from clusterman.simulator.simulator import SimulationMetadata
from clusterman.simulator.simulator import Simulator


@pytest.fixture
def simulator():
    with mock.patch('clusterman.simulator.simulator.PiecewiseConstantFunction'):
        return Simulator(
            SimulationMetadata('test', 'testing', 'mesos', 'test-tag'),
            arrow.get(0),
            arrow.get(3600),
            None,
            None,
        )


@pytest.fixture(autouse=True)
def sim_params():
    with staticconf.testing.PatchConfiguration({
        'join_delay_mean_seconds': 0,
        'join_delay_stdev_seconds': 0,
    }):
        yield
