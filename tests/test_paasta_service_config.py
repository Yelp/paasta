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
from mock import patch

from paasta_tools.chronos_tools import load_chronos_job_config
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.paasta_service_config import PaastaServiceConfig
from paasta_tools.utils import DeploymentsJson


TEST_SERVICE_NAME = 'example_happyhour'
TEST_SOA_DIR = 'fake_soa_dir'
TEST_CLUSTER_NAME = 'cluster'


def create_test_service():
    return PaastaServiceConfig(
        service=TEST_SERVICE_NAME,
        soa_dir=TEST_SOA_DIR,
        load_deployments=True,
    )


def deployment_json():
    return DeploymentsJson({'example_happyhour:paasta-norcal-prod2.main': {
        'docker_image': 'services-example_happyhour:paasta-ff682c43d474155d708cf751a63d63abb9788b5e',
        'desired_state': 'start',
        'force_bounce': None,
    }})


def marathon_cluster_config():
    """Return a sample dict to mock service_configuration_lib.read_extra_service_information"""
    return {
        'main': {
            'instances': 3, 'deploy_group': 'fake.non_canary',
            'cpus': 0.1, 'mem': 1000,
        },
        'canary': {
            'instances': 1, 'deploy_group': 'fake.canary',
            'cpus': 0.1, 'mem': 1000,
        },
    }


def chronos_cluster_config():
    """Return a sample dict to mock service_configuration_lib.read_extra_service_information"""
    return {
        'example_chronos_job': {
            'deploy_group': 'prod.non_canary', 'cpus': 0.1, 'mem': 200,
            'cmd': '/code/virtualenv_run/bin/python -m timeit',
            'schedule': 'R/2016-04-15T06:00:00Z/PT24H',
            'schedule_time_zone': 'America/Los_Angeles',
        },
        'example_child_job': {
            'parents': ['example_happyhour.example_chronos_job'],
            'deploy_group': 'prod.non_canary',
            'cmd': '/code/virtualenv_run/bin/python -m timeit',
        },
    }


@patch('paasta_tools.paasta_service_config.read_extra_service_information', autospec=True)
def test_marathon_instances(mock_read_extra_service_information):
    mock_read_extra_service_information.return_value = marathon_cluster_config()
    s = create_test_service()
    assert [i for i in s.instances(TEST_CLUSTER_NAME, 'marathon')] == ['main', 'canary']
    mock_read_extra_service_information.assert_called_once_with(
        extra_info='marathon-%s' % TEST_CLUSTER_NAME,
        service_name=TEST_SERVICE_NAME, soa_dir=TEST_SOA_DIR,
    )


@patch('paasta_tools.paasta_service_config.load_deployments_json', autospec=True)
@patch('paasta_tools.paasta_service_config.read_extra_service_information', autospec=True)
def test_marathon_instances_configs(
        mock_read_extra_service_information,
        mock_load_deployments_json,
):
    mock_read_extra_service_information.return_value = marathon_cluster_config()
    mock_load_deployments_json.return_value = deployment_json()
    s = create_test_service()
    expected = [
        MarathonServiceConfig(
            TEST_SERVICE_NAME, TEST_CLUSTER_NAME, 'main', {
                'port': None, 'vip': None,
                'lb_extras': {}, 'monitoring': {}, 'deploy': {}, 'data': {},
                'smartstack': {}, 'dependencies': {}, 'instances': 3,
                'deploy_group': 'fake.non_canary', 'cpus': 0.1, 'mem': 1000,
            }, {}, TEST_SOA_DIR,
        ),
        MarathonServiceConfig(
            TEST_SERVICE_NAME, TEST_CLUSTER_NAME, 'canary', {
                'port': None, 'vip': None,
                'lb_extras': {}, 'monitoring': {}, 'deploy': {}, 'data': {},
                'smartstack': {}, 'dependencies': {}, 'instances': 1,
                'deploy_group': 'fake.canary', 'cpus': 0.1, 'mem': 1000,
            }, {}, TEST_SOA_DIR,
        ),
    ]
    assert [i for i in s.instance_configs(TEST_CLUSTER_NAME, 'marathon')] == expected
    mock_read_extra_service_information.assert_called_once_with(
        extra_info='marathon-%s' % TEST_CLUSTER_NAME,
        service_name=TEST_SERVICE_NAME, soa_dir=TEST_SOA_DIR,
    )
    mock_load_deployments_json.assert_called_once_with(
        TEST_SERVICE_NAME,
        soa_dir=TEST_SOA_DIR,
    )


@patch('paasta_tools.paasta_service_config.load_deployments_json', autospec=True)
@patch('paasta_tools.marathon_tools.load_deployments_json', autospec=True)
@patch('paasta_tools.paasta_service_config.read_extra_service_information', autospec=True)
@patch('paasta_tools.marathon_tools.service_configuration_lib.read_extra_service_information', autospec=True)
def test_old_and_new_ways_load_the_same_marathon_configs(
        mock_marathon_tools_read_extra_service_information,
        mock_read_extra_service_information,
        mock_marathon_tools_load_deployments_json,
        mock_load_deployments_json,
):
    mock_read_extra_service_information.return_value = marathon_cluster_config()
    mock_marathon_tools_read_extra_service_information.return_value = marathon_cluster_config()
    mock_load_deployments_json.return_value = deployment_json()
    mock_marathon_tools_load_deployments_json.return_value = deployment_json()
    s = create_test_service()
    expected = [
        load_marathon_service_config(
            service=TEST_SERVICE_NAME, instance='main',
            cluster=TEST_CLUSTER_NAME, load_deployments=True, soa_dir=TEST_SOA_DIR,
        ),
        load_marathon_service_config(
            service=TEST_SERVICE_NAME, instance='canary',
            cluster=TEST_CLUSTER_NAME, load_deployments=True, soa_dir=TEST_SOA_DIR,
        ),
    ]
    assert [i for i in s.instance_configs(TEST_CLUSTER_NAME, 'marathon')] == expected


@patch('paasta_tools.paasta_service_config.load_deployments_json', autospec=True)
@patch('paasta_tools.chronos_tools.load_deployments_json', autospec=True)
@patch('paasta_tools.paasta_service_config.read_extra_service_information', autospec=True)
@patch('paasta_tools.chronos_tools.service_configuration_lib.read_extra_service_information', autospec=True)
def test_old_and_new_ways_load_the_same_chronos_configs(
        mock_chronos_tools_read_extra_service_information,
        mock_read_extra_service_information,
        mock_chronos_tools_load_deployments_json,
        mock_load_deployments_json,
):
    mock_read_extra_service_information.return_value = chronos_cluster_config()
    mock_chronos_tools_read_extra_service_information.return_value = chronos_cluster_config()
    mock_load_deployments_json.return_value = deployment_json()
    mock_chronos_tools_load_deployments_json.return_value = deployment_json()
    s = create_test_service()
    expected = [
        load_chronos_job_config(
            service=TEST_SERVICE_NAME, instance='example_chronos_job',
            cluster=TEST_CLUSTER_NAME, load_deployments=True, soa_dir=TEST_SOA_DIR,
        ),
        load_chronos_job_config(
            service=TEST_SERVICE_NAME, instance='example_child_job',
            cluster=TEST_CLUSTER_NAME, load_deployments=True, soa_dir=TEST_SOA_DIR,
        ),
    ]
    assert [i for i in s.instance_configs(TEST_CLUSTER_NAME, 'chronos')] == expected
