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
import argparse
import json

import mock
import pytest
import staticconf
import staticconf.testing
import yaml

import clusterman.config as config
from clusterman.config import POOL_NAMESPACE
from tests.conftest import mock_open


@pytest.fixture
def mock_config_files():
    with staticconf.testing.PatchConfiguration(
        {'cluster_config_directory': '/nail/whatever'}
    ), mock_open(
        config.get_pool_config_path('cluster-A', 'pool-1', 'mesos'),
        contents=yaml.dump({
            'resource_groups': 'cluster-A',
            'other_config': 18,
        }),
    ), mock_open(
        config.get_pool_config_path('cluster-A', 'pool-2', 'mesos'),
        contents=yaml.dump({
            'resource_groups': 'cluster-A',
            'other_config': 20,
        }),
    ), mock_open(
        config.get_pool_config_path('cluster-A', 'pool-2', 'kubernetes'),
        contents=yaml.dump({
            'resource_groups': 'cluster-A',
            'other_config': 29,
        }),
    ), mock_open(
        config.get_pool_config_path('cluster-B', 'pool-1', 'mesos'),
        contents=yaml.dump({
            'resource_groups': 'cluster-B',
            'other_config': 200,
            'autoscale_signal': {'branch_or_tag': 'v42'},
        }),
    ), mock_open(
        '/etc/no_cfg/clusterman.json',
        contents=json.dumps({
            'accessKeyId': 'foo',
            'secretAccessKey': 'bar',
            'region': 'nowhere-useful',
        })
    ):
        yield


@pytest.fixture(autouse=True)
def mock_config_namespaces():
    # To avoid polluting staticconf for other tests, and clear out stuff from conftest that mocks configuration
    with staticconf.testing.MockConfiguration(
        {},
        namespace=POOL_NAMESPACE.format(pool='pool-1', scheduler='mesos'),
    ), staticconf.testing.MockConfiguration(
        {},
        namespace=POOL_NAMESPACE.format(pool='pool-2', scheduler='mesos'),
    ), staticconf.testing.MockConfiguration(
        {
            'clusters': {
                'cluster-A': {
                    'mesos_url_api': 'service.leader',
                    'aws_region': 'us-test-3',
                },
            },
            'aws': {
                'access_key_file': '/etc/no_cfg/clusterman.json',
            }
        },
        namespace=staticconf.config.DEFAULT,
    ):
        yield


@pytest.mark.parametrize('cluster,pool,scheduler,tag', [
    ('cluster-A', 'pool-1', 'mesos', None),
    ('cluster-A', 'pool-2', 'mesos', 'v52'),
    ('cluster-A', 'pool-2', 'kubernetes', None),
    ('cluster-A', None, 'mesos', None),
])
def test_setup_config_cluster(cluster, pool, scheduler, tag, mock_config_files):
    args = argparse.Namespace(
        env_config_path='/nail/etc/config.yaml',
        cluster=cluster,
        pool=pool,
        scheduler=scheduler,
        signals_branch_or_tag=tag,
    )
    with mock.patch(
        'clusterman.config.load_cluster_pool_config',
        autospec=True,
    ) as mock_pool_load, mock.patch(
        'clusterman.config._load_module_configs',
    ) as mock_load_module_configs:

        config.setup_config(args)

        assert mock_load_module_configs.call_args == mock.call('/nail/etc/config.yaml')
        assert staticconf.read_string('aws.region') == 'us-test-3'
        if pool:
            assert mock_pool_load.call_args == mock.call(cluster, pool, scheduler, tag)
        else:
            assert mock_pool_load.call_count == 0
            if tag:
                assert staticconf.read_string('autoscale_signal.branch_or_tag') == tag


def test_setup_config_region_and_cluster():
    args = argparse.Namespace(
        env_config_path='/nail/etc/config.yaml',
        cluster='foo',
        aws_region='bar',
    )
    with mock.patch('clusterman.config._load_module_configs'), pytest.raises(argparse.ArgumentError):
        config.setup_config(args)


@mock.patch('clusterman.config._load_module_configs')
def test_setup_config_region(mock_load_module_configs, mock_config_files):
    args = argparse.Namespace(
        env_config_path='/nail/etc/config.yaml',
        aws_region='fake-region-A',
    )
    config.setup_config(args)
    assert staticconf.read_string('aws.region') == 'fake-region-A'
    assert mock_load_module_configs.call_args == mock.call('/nail/etc/config.yaml')


@pytest.mark.parametrize('cluster,pool,pool_other_config', [('cluster-B', 'pool-1', 200)])
def test_load_cluster_pool_config(cluster, pool, pool_other_config, mock_config_files):
    config.load_cluster_pool_config(cluster, pool, 'mesos', None)

    pool_namespace = POOL_NAMESPACE.format(pool=pool, scheduler='mesos')
    assert staticconf.read_int('other_config', namespace=pool_namespace) == pool_other_config
    assert staticconf.read_string('resource_groups', namespace=pool_namespace) == cluster
