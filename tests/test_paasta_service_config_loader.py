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

from paasta_tools.adhoc_tools import AdhocJobConfig
from paasta_tools.adhoc_tools import load_adhoc_job_config
from paasta_tools.chronos_tools import ChronosJobConfig
from paasta_tools.chronos_tools import load_chronos_job_config
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.marathon_tools import MarathonServiceConfig
from paasta_tools.paasta_service_config_loader import PaastaServiceConfigLoader
from paasta_tools.utils import DeploymentsJsonV2


TEST_SERVICE_NAME = "example_happyhour"
TEST_SOA_DIR = "fake_soa_dir"
TEST_CLUSTER_NAME = "cluster"


def create_test_service():
    return PaastaServiceConfigLoader(
        service=TEST_SERVICE_NAME, soa_dir=TEST_SOA_DIR, load_deployments=True
    )


def deployment_json():
    return DeploymentsJsonV2(
        service="test-service",
        config_dict={
            "deployments": {
                "cluster.non_canary": {
                    "docker_image": "some_image",
                    "git_sha": "some_sha",
                },
                "cluster.canary": {"docker_image": "some_image", "git_sha": "some_sha"},
            },
            "controls": {
                "example_happyhour:%s.sample_batch"
                % TEST_CLUSTER_NAME: {"desired_state": "start", "force_bounce": None},
                "example_happyhour:%s.interactive"
                % TEST_CLUSTER_NAME: {"desired_state": "start", "force_bounce": None},
                f"{TEST_SERVICE_NAME}:{TEST_CLUSTER_NAME}.main": {
                    "desired_state": "start",
                    "force_bounce": None,
                },
                f"{TEST_SERVICE_NAME}:{TEST_CLUSTER_NAME}.canary": {
                    "desired_state": "start",
                    "force_bounce": None,
                },
                f"{TEST_SERVICE_NAME}:{TEST_CLUSTER_NAME}.example_chronos_job": {
                    "desired_state": "start",
                    "force_bounce": None,
                },
                f"{TEST_SERVICE_NAME}:{TEST_CLUSTER_NAME}.example_child_job": {
                    "desired_state": "start",
                    "force_bounce": None,
                },
                f"{TEST_SERVICE_NAME}:{TEST_CLUSTER_NAME}.sample_batch": {
                    "desired_state": "start",
                    "force_bounce": None,
                },
                f"{TEST_SERVICE_NAME}:{TEST_CLUSTER_NAME}.interactive": {
                    "desired_state": "start",
                    "force_bounce": None,
                },
            },
        },
    )


def marathon_cluster_config():
    """Return a sample dict to mock service_configuration_lib.read_extra_service_information"""
    return {
        "main": {
            "instances": 3,
            "deploy_group": "{cluster}.non_canary",
            "cpus": 0.1,
            "mem": 1000,
        },
        "canary": {
            "instances": 1,
            "deploy_group": "{cluster}.canary",
            "cpus": 0.1,
            "mem": 1000,
        },
        "not_deployed": {
            "instances": 1,
            "deploy_group": "not_deployed",
            "cpus": 0.1,
            "mem": 1000,
        },
    }


def chronos_cluster_config():
    """Return a sample dict to mock service_configuration_lib.read_extra_service_information"""
    return {
        "example_chronos_job": {
            "deploy_group": "{cluster}.non_canary",
            "cpus": 0.1,
            "mem": 200,
            "cmd": "/bin/sleep 5s",
            "schedule": "R/2016-04-15T06:00:00Z/PT24H",
            "schedule_time_zone": "America/Los_Angeles",
        },
        "example_child_job": {
            "parents": ["example_happyhour.example_chronos_job"],
            "deploy_group": "{cluster}.non_canary",
            "cmd": "/bin/sleep 5s",
        },
        "not_deployed": {
            "deploy_group": "not_deployed",
            "cpus": 0.1,
            "mem": 1000,
            "schedule": "R/2016-04-15T06:00:00Z/PT24H",
            "schedule_time_zone": "America/Los_Angeles",
        },
    }


def adhoc_cluster_config():
    return {
        "sample_batch": {
            "deploy_group": "{cluster}.non_canary",
            "cpus": 0.1,
            "mem": 1000,
            "cmd": "/bin/sleep 5s",
        },
        "interactive": {"deploy_group": "{cluster}.non_canary", "mem": 1000},
        "not_deployed": {"deploy_group": "not_deployed"},
    }


@patch(
    "paasta_tools.paasta_service_config_loader.read_extra_service_information",
    autospec=True,
)
def test_marathon_instances(mock_read_extra_service_information):
    mock_read_extra_service_information.return_value = marathon_cluster_config()
    s = create_test_service()
    assert list(s.instances(TEST_CLUSTER_NAME, MarathonServiceConfig)) == [
        "main",
        "canary",
        "not_deployed",
    ]
    mock_read_extra_service_information.assert_called_once_with(
        extra_info="marathon-%s" % TEST_CLUSTER_NAME,
        service_name=TEST_SERVICE_NAME,
        soa_dir=TEST_SOA_DIR,
    )


@patch(
    "paasta_tools.paasta_service_config_loader.load_v2_deployments_json", autospec=True
)
@patch(
    "paasta_tools.paasta_service_config_loader.read_extra_service_information",
    autospec=True,
)
def test_marathon_instances_configs(
    mock_read_extra_service_information, mock_load_deployments_json
):
    mock_read_extra_service_information.return_value = marathon_cluster_config()
    mock_load_deployments_json.return_value = deployment_json()
    s = create_test_service()
    expected = [
        MarathonServiceConfig(
            service=TEST_SERVICE_NAME,
            cluster=TEST_CLUSTER_NAME,
            instance="main",
            config_dict={
                "port": None,
                "vip": None,
                "lb_extras": {},
                "monitoring": {},
                "deploy": {},
                "data": {},
                "smartstack": {},
                "dependencies": {},
                "instances": 3,
                "deploy_group": f"{TEST_CLUSTER_NAME}.non_canary",
                "cpus": 0.1,
                "mem": 1000,
            },
            branch_dict={
                "docker_image": "some_image",
                "desired_state": "start",
                "force_bounce": None,
                "git_sha": "some_sha",
            },
            soa_dir=TEST_SOA_DIR,
        ),
        MarathonServiceConfig(
            service=TEST_SERVICE_NAME,
            cluster=TEST_CLUSTER_NAME,
            instance="canary",
            config_dict={
                "port": None,
                "vip": None,
                "lb_extras": {},
                "monitoring": {},
                "deploy": {},
                "data": {},
                "smartstack": {},
                "dependencies": {},
                "instances": 1,
                "deploy_group": f"{TEST_CLUSTER_NAME}.canary",
                "cpus": 0.1,
                "mem": 1000,
            },
            branch_dict={
                "docker_image": "some_image",
                "desired_state": "start",
                "force_bounce": None,
                "git_sha": "some_sha",
            },
            soa_dir=TEST_SOA_DIR,
        ),
    ]
    assert (
        list(s.instance_configs(TEST_CLUSTER_NAME, MarathonServiceConfig)) == expected
    )
    mock_read_extra_service_information.assert_called_once_with(
        extra_info="marathon-%s" % TEST_CLUSTER_NAME,
        service_name=TEST_SERVICE_NAME,
        soa_dir=TEST_SOA_DIR,
    )
    mock_load_deployments_json.assert_called_once_with(
        TEST_SERVICE_NAME, soa_dir=TEST_SOA_DIR
    )


@patch(
    "paasta_tools.paasta_service_config_loader.load_v2_deployments_json", autospec=True
)
@patch(
    "paasta_tools.paasta_service_config_loader.read_extra_service_information",
    autospec=True,
)
def test_chronos_instances_configs(
    mock_read_extra_service_information, mock_load_deployments_json
):
    mock_read_extra_service_information.return_value = chronos_cluster_config()
    mock_load_deployments_json.return_value = deployment_json()
    s = create_test_service()
    expected = [
        ChronosJobConfig(
            service=TEST_SERVICE_NAME,
            cluster=TEST_CLUSTER_NAME,
            instance="example_chronos_job",
            config_dict={
                "port": None,
                "vip": None,
                "lb_extras": {},
                "monitoring": {},
                "deploy": {},
                "data": {},
                "smartstack": {},
                "dependencies": {},
                "deploy_group": "cluster.non_canary",
                "cpus": 0.1,
                "mem": 200,
                "cmd": "/bin/sleep 5s",
                "schedule": "R/2016-04-15T06:00:00Z/PT24H",
                "schedule_time_zone": "America/Los_Angeles",
            },
            branch_dict={
                "docker_image": "some_image",
                "desired_state": "start",
                "force_bounce": None,
                "git_sha": "some_sha",
            },
            soa_dir=TEST_SOA_DIR,
        ),
        ChronosJobConfig(
            service=TEST_SERVICE_NAME,
            cluster=TEST_CLUSTER_NAME,
            instance="example_child_job",
            config_dict={
                "port": None,
                "vip": None,
                "lb_extras": {},
                "monitoring": {},
                "deploy": {},
                "data": {},
                "smartstack": {},
                "dependencies": {},
                "parents": ["example_happyhour.example_chronos_job"],
                "deploy_group": "cluster.non_canary",
                "cmd": "/bin/sleep 5s",
            },
            branch_dict={
                "docker_image": "some_image",
                "desired_state": "start",
                "force_bounce": None,
                "git_sha": "some_sha",
            },
            soa_dir=TEST_SOA_DIR,
        ),
    ]
    assert list(s.instance_configs(TEST_CLUSTER_NAME, ChronosJobConfig)) == expected
    mock_read_extra_service_information.assert_called_once_with(
        extra_info="chronos-%s" % TEST_CLUSTER_NAME,
        service_name=TEST_SERVICE_NAME,
        soa_dir=TEST_SOA_DIR,
    )
    mock_load_deployments_json.assert_called_once_with(
        TEST_SERVICE_NAME, soa_dir=TEST_SOA_DIR
    )


@patch(
    "paasta_tools.paasta_service_config_loader.load_v2_deployments_json", autospec=True
)
@patch(
    "paasta_tools.paasta_service_config_loader.read_extra_service_information",
    autospec=True,
)
def test_adhoc_instances_configs(
    mock_read_extra_service_information, mock_load_deployments_json
):
    mock_read_extra_service_information.return_value = adhoc_cluster_config()
    mock_load_deployments_json.return_value = deployment_json()
    s = create_test_service()
    expected = [
        AdhocJobConfig(
            service=TEST_SERVICE_NAME,
            cluster=TEST_CLUSTER_NAME,
            instance="sample_batch",
            config_dict={
                "port": None,
                "vip": None,
                "lb_extras": {},
                "monitoring": {},
                "deploy": {},
                "data": {},
                "smartstack": {},
                "dependencies": {},
                "cmd": "/bin/sleep 5s",
                "deploy_group": "cluster.non_canary",
                "cpus": 0.1,
                "mem": 1000,
            },
            branch_dict={
                "docker_image": "some_image",
                "desired_state": "start",
                "force_bounce": None,
                "git_sha": "some_sha",
            },
            soa_dir=TEST_SOA_DIR,
        ),
        AdhocJobConfig(
            service=TEST_SERVICE_NAME,
            cluster=TEST_CLUSTER_NAME,
            instance="interactive",
            config_dict={
                "port": None,
                "vip": None,
                "lb_extras": {},
                "monitoring": {},
                "deploy": {},
                "data": {},
                "smartstack": {},
                "dependencies": {},
                "deploy_group": "cluster.non_canary",
                "mem": 1000,
            },
            branch_dict={
                "docker_image": "some_image",
                "desired_state": "start",
                "force_bounce": None,
                "git_sha": "some_sha",
            },
            soa_dir=TEST_SOA_DIR,
        ),
    ]
    for i in s.instance_configs(TEST_CLUSTER_NAME, AdhocJobConfig):
        print(i, i.cluster)
    assert list(s.instance_configs(TEST_CLUSTER_NAME, AdhocJobConfig)) == expected
    mock_read_extra_service_information.assert_called_once_with(
        extra_info="adhoc-%s" % TEST_CLUSTER_NAME,
        service_name=TEST_SERVICE_NAME,
        soa_dir=TEST_SOA_DIR,
    )
    mock_load_deployments_json.assert_called_once_with(
        TEST_SERVICE_NAME, soa_dir=TEST_SOA_DIR
    )


@patch(
    "paasta_tools.paasta_service_config_loader.load_v2_deployments_json", autospec=True
)
@patch("paasta_tools.marathon_tools.load_v2_deployments_json", autospec=True)
@patch(
    "paasta_tools.paasta_service_config_loader.read_extra_service_information",
    autospec=True,
)
@patch(
    "paasta_tools.marathon_tools.service_configuration_lib.read_extra_service_information",
    autospec=True,
)
def test_old_and_new_ways_load_the_same_marathon_configs(
    mock_marathon_tools_read_extra_service_information,
    mock_read_extra_service_information,
    mock_marathon_tools_load_deployments_json,
    mock_load_deployments_json,
):
    mock_read_extra_service_information.return_value = marathon_cluster_config()
    mock_marathon_tools_read_extra_service_information.return_value = (
        marathon_cluster_config()
    )
    mock_load_deployments_json.return_value = deployment_json()
    mock_marathon_tools_load_deployments_json.return_value = deployment_json()
    s = create_test_service()
    expected = [
        load_marathon_service_config(
            service=TEST_SERVICE_NAME,
            instance="main",
            cluster=TEST_CLUSTER_NAME,
            load_deployments=True,
            soa_dir=TEST_SOA_DIR,
        ),
        load_marathon_service_config(
            service=TEST_SERVICE_NAME,
            instance="canary",
            cluster=TEST_CLUSTER_NAME,
            load_deployments=True,
            soa_dir=TEST_SOA_DIR,
        ),
    ]
    assert (
        list(s.instance_configs(TEST_CLUSTER_NAME, MarathonServiceConfig)) == expected
    )


@patch(
    "paasta_tools.paasta_service_config_loader.load_v2_deployments_json", autospec=True
)
@patch("paasta_tools.chronos_tools.load_v2_deployments_json", autospec=True)
@patch(
    "paasta_tools.paasta_service_config_loader.read_extra_service_information",
    autospec=True,
)
@patch(
    "paasta_tools.chronos_tools.service_configuration_lib.read_extra_service_information",
    autospec=True,
)
def test_old_and_new_ways_load_the_same_chronos_configs(
    mock_chronos_tools_read_extra_service_information,
    mock_read_extra_service_information,
    mock_chronos_tools_load_deployments_json,
    mock_load_deployments_json,
):
    mock_read_extra_service_information.return_value = chronos_cluster_config()
    mock_chronos_tools_read_extra_service_information.return_value = (
        chronos_cluster_config()
    )
    mock_load_deployments_json.return_value = deployment_json()
    mock_chronos_tools_load_deployments_json.return_value = deployment_json()
    s = create_test_service()
    expected = [
        load_chronos_job_config(
            service=TEST_SERVICE_NAME,
            instance="example_chronos_job",
            cluster=TEST_CLUSTER_NAME,
            load_deployments=True,
            soa_dir=TEST_SOA_DIR,
        ),
        load_chronos_job_config(
            service=TEST_SERVICE_NAME,
            instance="example_child_job",
            cluster=TEST_CLUSTER_NAME,
            load_deployments=True,
            soa_dir=TEST_SOA_DIR,
        ),
    ]
    assert list(s.instance_configs(TEST_CLUSTER_NAME, ChronosJobConfig)) == expected


@patch(
    "paasta_tools.paasta_service_config_loader.load_v2_deployments_json", autospec=True
)
@patch("paasta_tools.adhoc_tools.load_v2_deployments_json", autospec=True)
@patch(
    "paasta_tools.paasta_service_config_loader.read_extra_service_information",
    autospec=True,
)
@patch(
    "paasta_tools.adhoc_tools.service_configuration_lib.read_extra_service_information",
    autospec=True,
)
def test_old_and_new_ways_load_the_same_adhoc_configs(
    mock_adhoc_tools_read_extra_service_information,
    mock_read_extra_service_information,
    mock_adhoc_tools_load_deployments_json,
    mock_load_deployments_json,
):
    mock_read_extra_service_information.return_value = adhoc_cluster_config()
    mock_adhoc_tools_read_extra_service_information.return_value = (
        adhoc_cluster_config()
    )
    mock_load_deployments_json.return_value = deployment_json()
    mock_adhoc_tools_load_deployments_json.return_value = deployment_json()
    s = create_test_service()
    expected = [
        load_adhoc_job_config(
            service=TEST_SERVICE_NAME,
            instance="sample_batch",
            cluster=TEST_CLUSTER_NAME,
            load_deployments=True,
            soa_dir=TEST_SOA_DIR,
        ),
        load_adhoc_job_config(
            service=TEST_SERVICE_NAME,
            instance="interactive",
            cluster=TEST_CLUSTER_NAME,
            load_deployments=True,
            soa_dir=TEST_SOA_DIR,
        ),
    ]
    assert list(s.instance_configs(TEST_CLUSTER_NAME, AdhocJobConfig)) == expected
