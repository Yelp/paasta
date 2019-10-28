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
from datetime import timedelta

import arrow
import mock
import pytest

from clusterman.aws.markets import InstanceMarket
from clusterman.reports.report_types import REPORT_TYPES
from clusterman.simulator.event import Event
from clusterman.simulator.simulated_aws_cluster import Instance
from clusterman.simulator.simulator import SimulationMetadata
from clusterman.simulator.simulator import Simulator


@pytest.fixture
def simulator():
    return Simulator(
        SimulationMetadata('test', 'testing', 'mesos', 'test-tag'),
        arrow.get(0),
        arrow.get(3600),
        None,
        None,
        billing_frequency=timedelta(hours=1),
    )


@pytest.fixture(params=[arrow.get(1820), arrow.get(3599), arrow.get(10000)])
def mock_instance(request):
    market = InstanceMarket('m4.4xlarge', 'us-west-1a')
    instance = Instance(market, arrow.get(0))
    instance.end_time = request.param
    return instance


@mock.patch('clusterman.simulator.simulator.colorlog.getLogger')
@pytest.mark.parametrize('evt_time', (arrow.get(10000), arrow.get(-1)))
def test_add_event_outside(mock_logger, simulator, evt_time):
    simulator.add_event(Event(evt_time))
    assert len(simulator.event_queue) == 2


def test_compute_instance_cost_no_breakpoints(simulator, mock_instance, fn):
    simulator.instance_prices[mock_instance.market] = fn
    simulator._compute_instance_cost(mock_instance)
    assert simulator.total_cost == 1


@pytest.mark.parametrize('bp_time', (arrow.get(-1), arrow.get(1800), arrow.get(5000)))
def test_compute_instance_cost_one_breakpoint(simulator, mock_instance, fn, bp_time):
    fn.add_breakpoint(bp_time, 3)
    simulator.instance_prices[mock_instance.market] = fn
    simulator._compute_instance_cost(mock_instance)
    assert simulator.total_cost == fn.call(arrow.get(0))


@pytest.mark.parametrize('early_bp,late_bp', [
    (False, False),
    (False, True),
    (True, False),
    (True, True),
])
def test_compute_instance_cost_multi_breakpoints(simulator, mock_instance, fn, early_bp, late_bp):
    fn.add_breakpoint(arrow.get(300), 3)
    fn.add_breakpoint(arrow.get(600), 5)
    if early_bp:
        fn.add_breakpoint(arrow.get(-100), 2)
    if late_bp:
        fn.add_breakpoint(arrow.get(7000), 2.5)
    simulator.instance_prices[mock_instance.market] = fn
    simulator.billing_frequency = timedelta(minutes=30)
    simulator._compute_instance_cost(mock_instance)
    assert simulator.total_cost == fn.call(arrow.get(0)) / 2 + fn.call(arrow.get(1800)) / 2


def test_compute_instance_cost_long_breakpoint_gap(simulator, mock_instance, fn):
    fn.add_breakpoint(arrow.get(400), 3)
    fn.add_breakpoint(arrow.get(3000), 5)
    simulator.instance_prices[mock_instance.market] = fn
    simulator.billing_frequency = timedelta(minutes=5)
    simulator._compute_instance_cost(mock_instance)
    assert simulator.total_cost == (
        (1 / 6 + 3 * 5 / 12)
        if mock_instance.end_time < arrow.get(3000)
        else 3
    )


@pytest.mark.parametrize('refund', [True, False])
def test_compute_instance_cost_outbid(simulator, mock_instance, fn, refund):
    fn.add_breakpoint(mock_instance.end_time.shift(minutes=-1), 3)
    mock_instance.bid_price = 2
    simulator.instance_prices[mock_instance.market] = fn
    simulator.billing_frequency = timedelta(minutes=30)
    simulator.refund_outbid = refund
    simulator._compute_instance_cost(mock_instance)
    assert simulator.total_cost == 1 / 2 if (refund and mock_instance.end_time < simulator.end_time) else 1


@pytest.mark.parametrize('refund', [True, False])
def test_compute_instance_cost_outbid_refund_irrelevant(simulator, mock_instance, fn, refund):
    fn.add_breakpoint(mock_instance.end_time.shift(minutes=1), 3)
    mock_instance.bid_price = 2
    simulator.instance_prices[mock_instance.market] = fn
    simulator.billing_frequency = timedelta(minutes=30)
    simulator.refund_outbid = refund
    simulator._compute_instance_cost(mock_instance)
    assert simulator.total_cost == 1


@pytest.mark.parametrize('report_type', list(REPORT_TYPES.keys()))
def test_get_data(simulator, report_type):
    simulator.get_data(report_type)


def test_get_data_invalid(simulator):
    with pytest.raises(ValueError):
        simulator.get_data('asdf')
