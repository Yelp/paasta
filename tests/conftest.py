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
import io
from contextlib import contextmanager

import mock
import pytest
import staticconf.testing
from clusterman_metrics import APP_METRICS
from clusterman_metrics import SYSTEM_METRICS

from clusterman.config import CREDENTIALS_NAMESPACE
from clusterman.math.piecewise import PiecewiseConstantFunction
from clusterman.monitoring_lib import yelp_meteorite


@contextmanager
def mock_open(filename, contents=None):
    """ This function modified from 'Revolution blahg':
    https://mapleoin.github.io/perma/mocking-python-file-open

    It is licensed under a Creative Commons Attribution 3.0 license
    (http://creativecommons.org/licenses/by/3.0/)
    """
    def mock_file(*args, **kwargs):
        if args[0] == filename:
            return io.StringIO(contents)
        else:
            mocked_file.stop()
            open_file = open(*args, **kwargs)
            mocked_file.start()
            return open_file
    mocked_file = mock.patch('builtins.open', mock_file)
    mocked_file.start()
    yield
    mocked_file.stop()


@pytest.fixture(autouse=True)
def main_clusterman_config():
    config = {
        'aws': {
            'access_key_file': '/etc/secrets',
            'region': 'us-west-2',
            'signals_bucket': 'the_bucket',
        },
        'autoscaling': {
            'setpoint': 0.7,
            'target_capacity_margin': 0.1,
            'default_signal_role': 'foo',
        },
        'batches': {
            'spot_prices': {
                'run_interval_seconds': 120,
                'dedupe_interval_seconds': 60,
            },
            'cluster_metrics': {
                'run_interval_seconds': 120,
            },
        },
        'drain_termination_timeout_seconds': {
            'sfr': 123,
        },
        'mesos_maintenance_timeout_seconds': 1,
        'clusters': {
            'mesos-test': {
                'mesos_master_fqdn': 'the.mesos.leader',
                'aws_region': 'us-west-2',
                'drain_queue_url': 'mesos-test-draining.com',
                'termination_queue_url': 'mesos-test-terminating.com',
                'warning_queue_url': 'mesos-test-warning.com',
            },
        },
        'sensu_config': [
            {
                'team': 'my_team',
                'runbook': 'y/my-runbook',
            }
        ],
        'autoscale_signal': {
            'name': 'DefaultSignal',
            'branch_or_tag': 'master',
            'period_minutes': 10,
        }
    }

    with staticconf.testing.MockConfiguration(config):
        yield


@pytest.fixture(autouse=True)
def clusterman_pool_config():
    config = {
        'resource_groups': [
            {
                'sfr': {
                    's3': {
                        'bucket': 'fake-bucket',
                        'prefix': 'none',
                    }
                },
            }, {
                'asg': {
                    'tag': 'puppet:role::paasta',
                },
            },
        ],
        'scaling_limits': {
            'min_capacity': 3,
            'max_capacity': 345,
            'max_weight_to_add': 200,
            'max_weight_to_remove': 10,
        },
        'sensu_config': [
            {
                'team': 'other-team',
                'runbook': 'y/their-runbook',
            }
        ],
        'autoscale_signal': {
            'name': 'BarSignal3',
            'branch_or_tag': 'v42',
            'period_minutes': 7,
            'required_metrics': [
                {'name': 'cpus_allocated', 'type': SYSTEM_METRICS, 'minute_range': 10},
                {'name': 'cost', 'type': APP_METRICS, 'minute_range': 30},
            ],
        }
    }
    with staticconf.testing.MockConfiguration(config, namespace='bar.mesos_config'):
        yield


@pytest.fixture(autouse=True)
def mock_aws_client_setup():
    config = {
        'accessKeyId': 'foo',
        'secretAccessKey': 'bar',
    }
    with staticconf.testing.MockConfiguration(config, namespace=CREDENTIALS_NAMESPACE):
        yield


@pytest.fixture(autouse=True)
def block_meteorite_emission():
    if yelp_meteorite:
        with yelp_meteorite.testcase():
            yield
    else:
        yield


@pytest.fixture
def fn():
    return PiecewiseConstantFunction(1)
