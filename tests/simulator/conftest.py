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
