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
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
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
                    "image_version": None,
                },
                "cluster.canary": {
                    "docker_image": "some_image",
                    "git_sha": "some_sha",
                    "image_version": None,
                },
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


def kubernetes_cluster_config():
    """Return a sample dict to mock paasta_tools.utils.load_service_instance_configs"""
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
    "paasta_tools.paasta_service_config_loader.load_service_instance_configs",
    autospec=True,
)
def test_kubernetes_instances(mock_load_service_instance_configs):
    mock_load_service_instance_configs.return_value = kubernetes_cluster_config()
    s = create_test_service()
    assert list(s.instances(TEST_CLUSTER_NAME, KubernetesDeploymentConfig)) == [
        "main",
        "canary",
        "not_deployed",
    ]
    mock_load_service_instance_configs.assert_called_once_with(
        service=TEST_SERVICE_NAME,
        instance_type="kubernetes",
        cluster=TEST_CLUSTER_NAME,
        soa_dir=TEST_SOA_DIR,
    )


@patch(
    "paasta_tools.paasta_service_config_loader.load_v2_deployments_json", autospec=True
)
@patch(
    "paasta_tools.paasta_service_config_loader.load_service_instance_configs",
    autospec=True,
)
def test_kubernetes_instances_configs(
    mock_load_service_instance_configs, mock_load_deployments_json
):
    mock_load_service_instance_configs.return_value = kubernetes_cluster_config()
    mock_load_deployments_json.return_value = deployment_json()
    s = create_test_service()
    expected = [
        KubernetesDeploymentConfig(
            service=TEST_SERVICE_NAME,
            cluster=TEST_CLUSTER_NAME,
            instance="main",
            config_dict={
                "port": None,
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
                "image_version": None,
            },
            soa_dir=TEST_SOA_DIR,
        ),
        KubernetesDeploymentConfig(
            service=TEST_SERVICE_NAME,
            cluster=TEST_CLUSTER_NAME,
            instance="canary",
            config_dict={
                "port": None,
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
                "image_version": None,
            },
            soa_dir=TEST_SOA_DIR,
        ),
    ]
    assert (
        list(s.instance_configs(TEST_CLUSTER_NAME, KubernetesDeploymentConfig))
        == expected
    )
    mock_load_service_instance_configs.assert_called_once_with(
        service=TEST_SERVICE_NAME,
        instance_type="kubernetes",
        cluster=TEST_CLUSTER_NAME,
        soa_dir=TEST_SOA_DIR,
    )
    mock_load_deployments_json.assert_called_once_with(
        TEST_SERVICE_NAME, soa_dir=TEST_SOA_DIR
    )


@patch(
    "paasta_tools.paasta_service_config_loader.load_v2_deployments_json", autospec=True
)
@patch(
    "paasta_tools.paasta_service_config_loader.load_service_instance_configs",
    autospec=True,
)
def test_adhoc_instances_configs(
    mock_load_service_instance_configs, mock_load_deployments_json
):
    mock_load_service_instance_configs.return_value = adhoc_cluster_config()
    mock_load_deployments_json.return_value = deployment_json()
    s = create_test_service()
    expected = [
        AdhocJobConfig(
            service=TEST_SERVICE_NAME,
            cluster=TEST_CLUSTER_NAME,
            instance="sample_batch",
            config_dict={
                "port": None,
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
                "image_version": None,
            },
            soa_dir=TEST_SOA_DIR,
        ),
        AdhocJobConfig(
            service=TEST_SERVICE_NAME,
            cluster=TEST_CLUSTER_NAME,
            instance="interactive",
            config_dict={
                "port": None,
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
                "image_version": None,
            },
            soa_dir=TEST_SOA_DIR,
        ),
    ]
    for i in s.instance_configs(TEST_CLUSTER_NAME, AdhocJobConfig):
        print(i, i.cluster)
    assert list(s.instance_configs(TEST_CLUSTER_NAME, AdhocJobConfig)) == expected
    mock_load_service_instance_configs.assert_called_once_with(
        service=TEST_SERVICE_NAME,
        instance_type="adhoc",
        cluster=TEST_CLUSTER_NAME,
        soa_dir=TEST_SOA_DIR,
    )
    mock_load_deployments_json.assert_called_once_with(
        TEST_SERVICE_NAME, soa_dir=TEST_SOA_DIR
    )


@patch(
    "paasta_tools.paasta_service_config_loader.load_v2_deployments_json", autospec=True
)
@patch("paasta_tools.adhoc_tools.load_v2_deployments_json", autospec=True)
@patch(
    "paasta_tools.paasta_service_config_loader.load_service_instance_configs",
    autospec=True,
)
@patch(
    "paasta_tools.adhoc_tools.load_service_instance_config",
    autospec=True,
)
def test_old_and_new_ways_load_the_same_adhoc_configs(
    mock_adhoc_tools_load_service_instance_config,
    mock_load_service_instance_configs,
    mock_adhoc_tools_load_deployments_json,
    mock_load_deployments_json,
):
    mock_load_service_instance_configs.return_value = adhoc_cluster_config()
    mock_adhoc_tools_load_service_instance_config.side_effect = [
        adhoc_cluster_config().get("sample_batch"),
        adhoc_cluster_config().get("interactive"),
    ]
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
