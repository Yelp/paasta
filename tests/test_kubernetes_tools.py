import functools
from base64 import b64encode
from copy import deepcopy
from typing import Any
from typing import Dict
from typing import List
from typing import Sequence

import asynctest
import mock
import pytest
from hypothesis import given
from hypothesis.strategies import floats
from hypothesis.strategies import integers
from kubernetes import client as kube_client
from kubernetes.client import V1Affinity
from kubernetes.client import V1AWSElasticBlockStoreVolumeSource
from kubernetes.client import V1Capabilities
from kubernetes.client import V1Container
from kubernetes.client import V1ContainerPort
from kubernetes.client import V1DeleteOptions
from kubernetes.client import V1Deployment
from kubernetes.client import V1DeploymentSpec
from kubernetes.client import V1DeploymentStrategy
from kubernetes.client import V1EnvVar
from kubernetes.client import V1EnvVarSource
from kubernetes.client import V1ExecAction
from kubernetes.client import V1HostPathVolumeSource
from kubernetes.client import V1HTTPGetAction
from kubernetes.client import V1Job
from kubernetes.client import V1JobSpec
from kubernetes.client import V1KeyToPath
from kubernetes.client import V1LabelSelector
from kubernetes.client import V1Lifecycle
from kubernetes.client import V1LifecycleHandler
from kubernetes.client import V1NodeAffinity
from kubernetes.client import V1NodeSelector
from kubernetes.client import V1NodeSelectorRequirement
from kubernetes.client import V1NodeSelectorTerm
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1PersistentVolumeClaim
from kubernetes.client import V1PersistentVolumeClaimSpec
from kubernetes.client import V1Pod
from kubernetes.client import V1PodAffinityTerm
from kubernetes.client import V1PodAntiAffinity
from kubernetes.client import V1PodDisruptionBudget
from kubernetes.client import V1PodSpec
from kubernetes.client import V1PodTemplateSpec
from kubernetes.client import V1PreferredSchedulingTerm
from kubernetes.client import V1Probe
from kubernetes.client import V1ProjectedVolumeSource
from kubernetes.client import V1ResourceRequirements
from kubernetes.client import V1RoleBinding
from kubernetes.client import V1RoleRef
from kubernetes.client import V1RollingUpdateDeployment
from kubernetes.client import V1Secret
from kubernetes.client import V1SecretKeySelector
from kubernetes.client import V1SecretVolumeSource
from kubernetes.client import V1SecurityContext
from kubernetes.client import V1ServiceAccount
from kubernetes.client import V1ServiceAccountList
from kubernetes.client import V1ServiceAccountTokenProjection
from kubernetes.client import V1StatefulSet
from kubernetes.client import V1StatefulSetSpec
from kubernetes.client import V1Subject
from kubernetes.client import V1TCPSocketAction
from kubernetes.client import V1TopologySpreadConstraint
from kubernetes.client import V1Volume
from kubernetes.client import V1VolumeMount
from kubernetes.client import V1VolumeProjection
from kubernetes.client import V2CrossVersionObjectReference
from kubernetes.client import V2HorizontalPodAutoscaler
from kubernetes.client import V2HorizontalPodAutoscalerSpec
from kubernetes.client import V2MetricIdentifier
from kubernetes.client import V2MetricSpec
from kubernetes.client import V2MetricTarget
from kubernetes.client import V2ResourceMetricSource
from kubernetes.client.models.v2_object_metric_source import (
    V2ObjectMetricSource,
)
from kubernetes.client.rest import ApiException
from requests.exceptions import ConnectionError

from paasta_tools import kubernetes_tools
from paasta_tools.contrib.get_running_task_allocation import (
    get_kubernetes_metadata as task_allocation_get_kubernetes_metadata,
)
from paasta_tools.contrib.get_running_task_allocation import (
    get_pod_pool as task_allocation_get_pod_pool,
)
from paasta_tools.kubernetes_tools import add_volumes_for_authenticating_services
from paasta_tools.kubernetes_tools import allowlist_denylist_to_requirements
from paasta_tools.kubernetes_tools import create_custom_resource
from paasta_tools.kubernetes_tools import create_deployment
from paasta_tools.kubernetes_tools import create_pod_disruption_budget
from paasta_tools.kubernetes_tools import create_secret
from paasta_tools.kubernetes_tools import create_secret_signature
from paasta_tools.kubernetes_tools import create_stateful_set
from paasta_tools.kubernetes_tools import ensure_namespace
from paasta_tools.kubernetes_tools import ensure_paasta_api_rolebinding
from paasta_tools.kubernetes_tools import ensure_paasta_namespace_limits
from paasta_tools.kubernetes_tools import ensure_service_account
from paasta_tools.kubernetes_tools import filter_nodes_by_blacklist
from paasta_tools.kubernetes_tools import filter_pods_by_service_instance
from paasta_tools.kubernetes_tools import force_delete_pods
from paasta_tools.kubernetes_tools import get_active_versions_for_service
from paasta_tools.kubernetes_tools import get_all_nodes
from paasta_tools.kubernetes_tools import get_all_pods
from paasta_tools.kubernetes_tools import get_annotations_for_kubernetes_service
from paasta_tools.kubernetes_tools import get_kubernetes_app_by_name
from paasta_tools.kubernetes_tools import get_kubernetes_app_deploy_status
from paasta_tools.kubernetes_tools import get_kubernetes_secret_env_variables
from paasta_tools.kubernetes_tools import get_kubernetes_secret_hashes
from paasta_tools.kubernetes_tools import get_kubernetes_secret_volumes
from paasta_tools.kubernetes_tools import get_kubernetes_services_running_here
from paasta_tools.kubernetes_tools import get_kubernetes_services_running_here_for_nerve
from paasta_tools.kubernetes_tools import get_nodes_grouped_by_attribute
from paasta_tools.kubernetes_tools import get_paasta_secret_name
from paasta_tools.kubernetes_tools import get_paasta_secret_signature_name
from paasta_tools.kubernetes_tools import get_secret
from paasta_tools.kubernetes_tools import get_secret_name_from_ref
from paasta_tools.kubernetes_tools import get_secret_signature
from paasta_tools.kubernetes_tools import get_service_account_name
from paasta_tools.kubernetes_tools import group_pods_by_service_instance
from paasta_tools.kubernetes_tools import InvalidKubernetesConfig
from paasta_tools.kubernetes_tools import is_node_ready
from paasta_tools.kubernetes_tools import is_pod_ready
from paasta_tools.kubernetes_tools import KubeAffinityCondition
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import KubeContainerResources
from paasta_tools.kubernetes_tools import KubeCustomResource
from paasta_tools.kubernetes_tools import KubeDeployment
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfigDict
from paasta_tools.kubernetes_tools import KubernetesDeployStatus
from paasta_tools.kubernetes_tools import KubernetesServiceRegistration
from paasta_tools.kubernetes_tools import list_all_deployments
from paasta_tools.kubernetes_tools import list_all_paasta_deployments
from paasta_tools.kubernetes_tools import list_custom_resources
from paasta_tools.kubernetes_tools import load_kubernetes_service_config
from paasta_tools.kubernetes_tools import load_kubernetes_service_config_no_cache
from paasta_tools.kubernetes_tools import max_unavailable
from paasta_tools.kubernetes_tools import mode_to_int
from paasta_tools.kubernetes_tools import paasta_prefixed
from paasta_tools.kubernetes_tools import pod_disruption_budget_for_service_instance
from paasta_tools.kubernetes_tools import pods_for_service_instance
from paasta_tools.kubernetes_tools import raw_selectors_to_requirements
from paasta_tools.kubernetes_tools import sanitise_kubernetes_name
from paasta_tools.kubernetes_tools import set_instances_for_kubernetes_service
from paasta_tools.kubernetes_tools import update_custom_resource
from paasta_tools.kubernetes_tools import update_deployment
from paasta_tools.kubernetes_tools import update_secret
from paasta_tools.kubernetes_tools import update_secret_signature
from paasta_tools.kubernetes_tools import update_stateful_set
from paasta_tools.long_running_service_tools import METRICS_PROVIDER_CPU
from paasta_tools.long_running_service_tools import METRICS_PROVIDER_GUNICORN
from paasta_tools.long_running_service_tools import METRICS_PROVIDER_PISCINA
from paasta_tools.long_running_service_tools import METRICS_PROVIDER_UWSGI
from paasta_tools.long_running_service_tools import METRICS_PROVIDER_UWSGI_V2
from paasta_tools.long_running_service_tools import ServiceNamespaceConfig
from paasta_tools.secret_tools import SHARED_SECRET_SERVICE
from paasta_tools.utils import AwsEbsVolume
from paasta_tools.utils import CAPS_DROP
from paasta_tools.utils import DeploymentVersion
from paasta_tools.utils import DockerVolume
from paasta_tools.utils import PersistentVolume
from paasta_tools.utils import ProjectedSAVolume
from paasta_tools.utils import SecretVolume
from paasta_tools.utils import SecretVolumeItem
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import TopologySpreadConstraintDict


def test_force_delete_pods():
    mock_pod_1 = mock.MagicMock(
        metadata=mock.MagicMock(
            labels={
                "yelp.com/paasta_service": "srv1",
                "yelp.com/paasta_instance": "instance1",
                "paasta.yelp.com/service": "srv1",
                "paasta.yelp.com/instance": "instance1",
            }
        )
    )
    mock_pod_2 = mock.MagicMock(
        metadata=mock.MagicMock(
            labels={
                "yelp.com/paasta_service": "srv1",
                "yelp.com/paasta_instance": "instance1",
                "paasta.yelp.com/service": "srv1",
                "paasta.yelp.com/instance": "instance1",
            }
        )
    )
    mock_pod_1.metadata.name = "pod_1"
    mock_pod_2.metadata.name = "pod_2"
    mock_pods = [mock_pod_1, mock_pod_2]
    mock_client = mock.Mock()

    with mock.patch(
        "paasta_tools.kubernetes_tools.pods_for_service_instance",
        autospec=True,
        return_value=mock_pods,
    ):
        force_delete_pods("srv1", "srv1", "instance1", "namespace", mock_client)
        body = V1DeleteOptions()

        assert mock_client.core.delete_namespaced_pod.call_count == 2
        assert mock_client.core.delete_namespaced_pod.call_args_list[0] == mock.call(
            "pod_1", "namespace", body=body, grace_period_seconds=0
        )
        assert mock_client.core.delete_namespaced_pod.call_args_list[1] == mock.call(
            "pod_2", "namespace", body=body, grace_period_seconds=0
        )


def test_load_kubernetes_service_config_no_cache():
    with mock.patch(
        "service_configuration_lib.read_service_configuration", autospec=True
    ) as mock_read_service_configuration, mock.patch(
        "paasta_tools.kubernetes_tools.load_service_instance_config", autospec=True
    ) as mock_load_service_instance_config, mock.patch(
        "paasta_tools.kubernetes_tools.load_v2_deployments_json", autospec=True
    ) as mock_load_v2_deployments_json, mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig", autospec=True
    ) as mock_kube_deploy_config:
        mock_config = {"freq": "108.9"}
        mock_load_service_instance_config.return_value = mock_config
        mock_read_service_configuration.return_value = {}
        ret = load_kubernetes_service_config_no_cache(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            load_deployments=False,
            soa_dir="/nail/blah",
        )
        mock_load_service_instance_config.assert_called_with(
            service="kurupt",
            instance="fm",
            instance_type="kubernetes",
            cluster="brentford",
            soa_dir="/nail/blah",
        )
        mock_kube_deploy_config.assert_called_with(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            config_dict={"freq": "108.9"},
            branch_dict=None,
            soa_dir="/nail/blah",
        )
        assert not mock_load_v2_deployments_json.called
        assert ret == mock_kube_deploy_config.return_value

        mock_kube_deploy_config.reset_mock()
        ret = load_kubernetes_service_config_no_cache(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            load_deployments=True,
            soa_dir="/nail/blah",
        )
        mock_load_v2_deployments_json.assert_called_with(
            service="kurupt", soa_dir="/nail/blah"
        )
        mock_kube_deploy_config.assert_called_with(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            config_dict={"freq": "108.9"},
            branch_dict=mock_load_v2_deployments_json.return_value.get_branch_dict(),
            soa_dir="/nail/blah",
        )
        assert ret == mock_kube_deploy_config.return_value


def test_load_kubernetes_service_config():
    with mock.patch(
        "paasta_tools.kubernetes_tools.load_kubernetes_service_config_no_cache",
        autospec=True,
    ) as mock_load_kubernetes_service_config_no_cache:
        ret = load_kubernetes_service_config(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            load_deployments=True,
            soa_dir="/nail/blah",
        )
        assert ret == mock_load_kubernetes_service_config_no_cache.return_value


class TestKubernetesDeploymentConfig:
    @pytest.fixture(autouse=True)
    def mock_load_kube_config(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.kube_config.load_kube_config",
            autospec=True,
        ) as m:
            yield m

    def setup_method(self, method):
        mock_config_dict = KubernetesDeploymentConfigDict(
            bounce_method="crossover",
            instances=3,
        )
        self.deployment = KubernetesDeploymentConfig(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            config_dict=mock_config_dict,
            branch_dict=None,
            soa_dir="/nail/blah",
        )

    def test_copy(self):
        assert self.deployment.copy() == self.deployment
        assert self.deployment.copy() is not self.deployment

    def test_get_cmd_returns_None(self):
        deployment = KubernetesDeploymentConfig(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            config_dict={"cmd": None},
            branch_dict=None,
            soa_dir="/nail/blah",
        )
        assert deployment.get_cmd() is None

    def test_get_cmd_converts_str_to_list(self):
        deployment = KubernetesDeploymentConfig(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            config_dict={"cmd": "/bin/echo hi"},
            branch_dict=None,
            soa_dir="/nail/blah",
        )
        assert deployment.get_cmd() == ["sh", "-c", "/bin/echo hi"]

    def test_get_cmd_list(self):
        deployment = KubernetesDeploymentConfig(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            config_dict={"cmd": "/bin/echo hi"},
            branch_dict=None,
            soa_dir="/nail/blah",
        )
        assert deployment.get_cmd() == ["sh", "-c", "/bin/echo hi"]

    def test_get_bounce_method(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_aws_ebs_volumes",
            autospec=True,
        ) as mock_get_aws_ebs_volumes:
            # if ebs we must downthenup for now as we need to free up the EBS for the new instance
            mock_get_aws_ebs_volumes.return_value = ["some-ebs"]
            with pytest.raises(Exception):
                self.deployment.get_bounce_method()

    @pytest.mark.parametrize(
        "bounce_method,bounce_margin_factor,expected_strategy,expected_rolling_update_deploy",
        [
            (
                "crossover",
                1,
                "RollingUpdate",
                V1RollingUpdateDeployment(max_surge="100%", max_unavailable="0%"),
            ),
            (
                "crossover",
                0.3,
                "RollingUpdate",
                V1RollingUpdateDeployment(max_surge="100%", max_unavailable="70%"),
            ),
            # b_m_f does not actually contribute to settings for brutal
            (
                "brutal",
                0.5,
                "RollingUpdate",
                V1RollingUpdateDeployment(max_surge="100%", max_unavailable="100%"),
            ),
            ("downthenup", 1, "Recreate", None),
        ],
    )
    def test_get_deployment_strategy(
        self,
        bounce_method,
        bounce_margin_factor,
        expected_strategy,
        expected_rolling_update_deploy,
    ):
        with mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_bounce_method",
            autospec=True,
            return_value=bounce_method,
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_bounce_margin_factor",
            autospec=True,
            return_value=bounce_margin_factor,
        ):
            assert (
                self.deployment.get_deployment_strategy_config()
                == V1DeploymentStrategy(
                    type=expected_strategy,
                    rolling_update=expected_rolling_update_deploy,
                )
            )

    def test_get_sanitised_volume_name(self):
        assert (
            self.deployment.get_sanitised_volume_name("/var/tmp")
            == "slash-varslash-tmp"
        )
        assert (
            self.deployment.get_sanitised_volume_name("/var/tmp/")
            == "slash-varslash-tmp"
        )
        assert (
            self.deployment.get_sanitised_volume_name("/var/tmp_file.json")
            == "slash-varslash-tmp--filedot-json"
        )
        assert (
            self.deployment.get_sanitised_volume_name("/var/tmp_file.json", 20)
            == "slash-varslash--1953"
        )

    @pytest.mark.parametrize(
        "config_dict, system_cfg_get_enable_nerve, expected_get_nerve",
        [
            ({"bounce_health_params": {"check_haproxy": True}}, True, True),
            ({"bounce_health_params": {"check_haproxy": True}}, False, True),
            ({"bounce_health_params": {"check_haproxy": False}}, True, False),
            ({"bounce_health_params": {"check_envoy": False}}, True, True),
            ({}, True, True),
        ],
    )
    def test_get_enable_nerve_readiness_check(
        self, config_dict, system_cfg_get_enable_nerve, expected_get_nerve
    ):
        deployment = KubernetesDeploymentConfig(
            service="my-service",
            instance="my-instance",
            cluster="mega-cluster",
            config_dict=config_dict,
            branch_dict=None,
            soa_dir="/nail/blah",
        )
        mock_system_paasta_config = mock.Mock()
        mock_system_paasta_config.get_enable_nerve_readiness_check.return_value = (
            system_cfg_get_enable_nerve
        )
        assert (
            deployment.get_enable_nerve_readiness_check(mock_system_paasta_config)
            == expected_get_nerve
        )

    @pytest.mark.parametrize(
        "config_dict, system_cfg_get_enable_envoy, expected_get_envoy",
        [
            ({"bounce_health_params": {"check_envoy": True}}, True, True),
            ({"bounce_health_params": {"check_envoy": True}}, False, True),
            ({"bounce_health_params": {"check_envoy": False}}, True, False),
            ({"bounce_health_params": {"check_haproxy": False}}, True, True),
            ({}, True, True),
        ],
    )
    def test_get_enable_envoy_readiness_check(
        self, config_dict, system_cfg_get_enable_envoy, expected_get_envoy
    ):
        deployment = KubernetesDeploymentConfig(
            service="my-service",
            instance="my-instance",
            cluster="mega-cluster",
            config_dict=config_dict,
            branch_dict=None,
            soa_dir="/nail/blah",
        )
        mock_system_paasta_config = mock.Mock()
        mock_system_paasta_config.get_enable_envoy_readiness_check.return_value = (
            system_cfg_get_enable_envoy
        )
        assert (
            deployment.get_enable_envoy_readiness_check(mock_system_paasta_config)
            == expected_get_envoy
        )

    @pytest.mark.parametrize(
        "enable_envoy_check, enable_nerve_check, expected_cmd",
        [
            (
                True,
                True,
                ["/check_proxy_up.sh", "--enable-smartstack", "--enable-envoy"],
            ),
            (
                True,
                False,
                [
                    "/check_proxy_up.sh",
                    "--enable-envoy",
                    "--envoy-check-mode",
                    "eds-dir",
                ],
            ),
            (False, True, ["/check_smartstack_up.sh"]),
        ],
    )
    def test_get_readiness_check_script(
        self, enable_envoy_check, enable_nerve_check, expected_cmd
    ):
        fake_system_paasta_config = SystemPaastaConfig({}, "/some/fake/dir")
        with mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_enable_envoy_readiness_check",
            autospec=True,
            return_value=enable_envoy_check,
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_enable_nerve_readiness_check",
            autospec=True,
            return_value=enable_nerve_check,
        ):
            assert (
                self.deployment.get_readiness_check_script(fake_system_paasta_config)
                == expected_cmd
            )

    def test_get_sidecar_containers(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_registrations",
            autospec=True,
            return_value=["universal.credit"],
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_kubernetes_environment",
            autospec=True,
            return_value=[],
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_sanitised_volume_name",
            autospec=True,
            return_value="sane-name",
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_enable_nerve_readiness_check",
            autospec=True,
            return_value=False,
        ) as mock_get_enable_nerve_readiness_check, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_enable_envoy_readiness_check",
            autospec=True,
            return_value=False,
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_readiness_check_script",
            autospec=True,
            return_value=["/nail/blah.sh"],
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_sidecar_resource_requirements",
            autospec=True,
            return_value={
                "limits": {
                    "cpu": 0.1,
                    "memory": "1024Mi",
                    "ephemeral-storage": "256Mi",
                },
                "requests": {
                    "cpu": 0.1,
                    "memory": "1024Mi",
                    "ephemeral-storage": "256Mi",
                },
            },
        ):
            mock_system_config = mock.Mock(
                get_hacheck_sidecar_image_url=mock.Mock(
                    return_value="some-docker-image"
                ),
            )
            mock_service_namespace = mock.Mock(
                is_in_smartstack=mock.Mock(return_value=False)
            )
            hacheck_sidecar_volumes = [
                DockerVolume(
                    hostPath="/nail/blah", containerPath="/nail/foo", mode="RO"
                ),
                DockerVolume(
                    hostPath="/nail/thing", containerPath="/nail/bar", mode="RO"
                ),
            ]
            expected_volumes = [
                V1VolumeMount(mount_path="/nail/foo", name="sane-name", read_only=True),
                V1VolumeMount(mount_path="/nail/bar", name="sane-name", read_only=True),
            ]

            assert (
                self.deployment.get_sidecar_containers(
                    mock_system_config, mock_service_namespace, hacheck_sidecar_volumes
                )
                == []
            )

            mock_service_namespace = mock.Mock(
                is_in_smartstack=mock.Mock(return_value=True)
            )

            ret = self.deployment.get_sidecar_containers(
                mock_system_config, mock_service_namespace, hacheck_sidecar_volumes
            )
            expected = [
                V1Container(
                    env=[
                        V1EnvVar(name="MESH_REGISTRATIONS", value="universal.credit"),
                    ],
                    image="some-docker-image",
                    lifecycle=V1Lifecycle(
                        pre_stop=V1LifecycleHandler(
                            _exec=V1ExecAction(
                                command=[
                                    "/bin/sh",
                                    "-c",
                                    "/usr/bin/hadown " "universal.credit; sleep " "31",
                                ]
                            )
                        )
                    ),
                    resources=V1ResourceRequirements(
                        limits={
                            "cpu": 0.1,
                            "memory": "1024Mi",
                            "ephemeral-storage": "256Mi",
                        },
                        requests={
                            "cpu": 0.1,
                            "memory": "1024Mi",
                            "ephemeral-storage": "256Mi",
                        },
                    ),
                    name="hacheck",
                    ports=[V1ContainerPort(container_port=6666)],
                    volume_mounts=expected_volumes,
                )
            ]
            assert ret == expected
            mock_get_enable_nerve_readiness_check.return_value = True
            mock_system_config = mock.Mock(
                get_hacheck_sidecar_image_url=mock.Mock(
                    return_value="some-docker-image"
                ),
                get_hacheck_match_initial_delay=mock.Mock(return_value=False),
                get_readiness_check_prefix_template=mock.Mock(return_value=[]),
            )
            ret = self.deployment.get_sidecar_containers(
                mock_system_config, mock_service_namespace, hacheck_sidecar_volumes
            )
            expected = [
                V1Container(
                    env=[
                        V1EnvVar(name="MESH_REGISTRATIONS", value="universal.credit"),
                    ],
                    image="some-docker-image",
                    lifecycle=V1Lifecycle(
                        pre_stop=V1LifecycleHandler(
                            _exec=V1ExecAction(
                                command=[
                                    "/bin/sh",
                                    "-c",
                                    "/usr/bin/hadown " "universal.credit; sleep " "31",
                                ]
                            )
                        )
                    ),
                    name="hacheck",
                    resources=V1ResourceRequirements(
                        limits={
                            "cpu": 0.1,
                            "memory": "1024Mi",
                            "ephemeral-storage": "256Mi",
                        },
                        requests={
                            "cpu": 0.1,
                            "memory": "1024Mi",
                            "ephemeral-storage": "256Mi",
                        },
                    ),
                    ports=[V1ContainerPort(container_port=6666)],
                    volume_mounts=expected_volumes,
                    readiness_probe=V1Probe(
                        _exec=V1ExecAction(
                            command=["/nail/blah.sh", "8888", "universal.credit"]
                        ),
                        initial_delay_seconds=10,
                        period_seconds=10,
                    ),
                )
            ]
            assert ret == expected

    def test_get_sidecar_resource_requirements(self):
        self.deployment.config_dict["sidecar_resource_requirements"] = {
            "hacheck": {
                "requests": {
                    "cpu": 0.2,
                    "memory": "1024Mi",
                    "ephemeral-storage": "256Mi",
                },
                "limits": {
                    "cpu": 0.3,
                    "memory": "1025Mi",
                    "ephemeral-storage": "257Mi",
                },
            }
        }
        system_paasta_config = mock.Mock()

        assert self.deployment.get_sidecar_resource_requirements(
            "hacheck", system_paasta_config
        ) == V1ResourceRequirements(
            limits={"cpu": 0.3, "memory": "1025Mi", "ephemeral-storage": "257Mi"},
            requests={"cpu": 0.2, "memory": "1024Mi", "ephemeral-storage": "256Mi"},
        )

    def test_get_sidecar_resource_requirements_default_limits(self):
        """When limits is unspecified, it should default to the request"""
        self.deployment.config_dict["sidecar_resource_requirements"] = {
            "hacheck": {
                "requests": {
                    "cpu": 0.2,
                    "memory": "1025Mi",
                    "ephemeral-storage": "257Mi",
                },
            }
        }
        system_paasta_config = mock.Mock()
        assert self.deployment.get_sidecar_resource_requirements(
            "hacheck", system_paasta_config
        ) == V1ResourceRequirements(
            limits={"cpu": 0.2, "memory": "1025Mi", "ephemeral-storage": "257Mi"},
            requests={"cpu": 0.2, "memory": "1025Mi", "ephemeral-storage": "257Mi"},
        )

    def test_get_sidecar_resource_requirements_default_requirements(self):
        """When request is unspecified, it should default to the 0.1, 1024Mi, 256Mi."""
        try:
            del self.deployment.config_dict["sidecar_resource_requirements"]
        except KeyError:
            pass

        system_paasta_config = mock.Mock(
            get_sidecar_requirements_config=mock.Mock(
                return_value={
                    "hacheck": {
                        "cpu": 0.1,
                        "memory": "512Mi",
                        "ephemeral-storage": "256Mi",
                    },
                }
            )
        )
        assert self.deployment.get_sidecar_resource_requirements(
            "hacheck", system_paasta_config
        ) == V1ResourceRequirements(
            limits={"cpu": 0.1, "memory": "512Mi", "ephemeral-storage": "256Mi"},
            requests={"cpu": 0.1, "memory": "512Mi", "ephemeral-storage": "256Mi"},
        )

    def test_get_sidecar_resource_requirements_limits_override_default_requirements(
        self,
    ):
        """When limit is partially specified, it should use the default requests, and limits should be the same except for the overridden value."""
        self.deployment.config_dict["sidecar_resource_requirements"] = {
            "hacheck": {
                "limits": {"cpu": 1.0},
            }
        }
        system_paasta_config = mock.Mock(
            get_sidecar_requirements_config=mock.Mock(
                return_value={
                    "hacheck": {
                        "cpu": 0.1,
                        "memory": "1024Mi",
                        "ephemeral-storage": "256Mi",
                    },
                }
            )
        )
        assert self.deployment.get_sidecar_resource_requirements(
            "hacheck", system_paasta_config
        ) == V1ResourceRequirements(
            limits={"cpu": 1.0, "memory": "1024Mi", "ephemeral-storage": "256Mi"},
            requests={"cpu": 0.1, "memory": "1024Mi", "ephemeral-storage": "256Mi"},
        )

    def test_get_env(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.LongRunningServiceConfig.get_env",
            autospec=True,
            return_value={"hello": "world"},
        ):
            assert self.deployment.get_env() == {
                "hello": "world",
                "PAASTA_SOA_CONFIGS_SHA": "fake_soa_git_sha",
            }

    def test_get_container_env(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_env",
            autospec=True,
            return_value={
                "mc": "grindah",
                "dj": "beats",
                "A": "SECRET(123)",
                "B": "SHAREDSECRET(456)",
            },
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_kubernetes_environment",
            autospec=True,
            return_value=[V1EnvVar(name="manager", value="chabuddy")],
        ), mock.patch(
            "paasta_tools.kubernetes_tools.is_secret_ref", autospec=True
        ) as mock_is_secret_ref, mock.patch(
            "paasta_tools.kubernetes_tools.is_shared_secret", autospec=True
        ) as mock_is_shared_secret, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_kubernetes_secret_env_vars",
            autospec=True,
            return_value=[],
        ) as mock_get_kubernetes_secret_env_vars:
            mock_is_secret_ref.side_effect = lambda x: True if "SECRET" in x else False
            mock_is_shared_secret.side_effect = (
                lambda x: False if not x.startswith("SHARED") else True
            )
            expected = [
                V1EnvVar(name="mc", value="grindah"),
                V1EnvVar(name="dj", value="beats"),
                V1EnvVar(name="manager", value="chabuddy"),
            ]
            assert expected == self.deployment.get_container_env()
            mock_get_kubernetes_secret_env_vars.assert_called_with(
                self.deployment,
                secret_env_vars={"A": "SECRET(123)"},
                shared_secret_env_vars={"B": "SHAREDSECRET(456)"},
            )

    def test_get_kubernetes_environment(self):
        ret = self.deployment.get_kubernetes_environment()
        assert "PAASTA_POD_IP" in [env.name for env in ret]
        assert "POD_NAME" in [env.name for env in ret]
        assert "PAASTA_CLUSTER" in [env.name for env in ret]

    def test_get_resource_requirements(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_cpus",
            autospec=True,
            return_value=0.3,
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_cpu_burst_add",
            autospec=True,
            return_value=1,
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_mem",
            autospec=True,
            return_value=2048,
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_disk",
            autospec=True,
            return_value=4096,
        ):
            assert (
                self.deployment.get_resource_requirements()
                == V1ResourceRequirements(
                    limits={
                        "cpu": 1.3,
                        "memory": "2048Mi",
                        "ephemeral-storage": "4096Mi",
                    },
                    requests={
                        "cpu": 0.3,
                        "memory": "2048Mi",
                        "ephemeral-storage": "4096Mi",
                    },
                )
            )

    @pytest.mark.parametrize(
        "prometheus_port,expected_ports",
        [
            (None, [8888]),
            (8888, [8888]),
            (29143, [8888, 29143]),
        ],
    )
    def test_get_kubernetes_containers(self, prometheus_port, expected_ports):
        with mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_docker_url",
            autospec=True,
        ) as mock_get_docker_url, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_cmd",
            autospec=True,
        ) as mock_get_cmd, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_args",
            autospec=True,
        ) as mock_get_args, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_resource_requirements",
            autospec=True,
        ) as mock_get_resource_requirements, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_container_env",
            autospec=True,
        ) as mock_get_container_env, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_sanitised_service_name",
            autospec=True,
            return_value="kurupt",
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_sanitised_instance_name",
            autospec=True,
            return_value="fm",
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_volume_mounts",
            autospec=True,
        ) as mock_get_volume_mounts, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_sidecar_containers",
            autospec=True,
            return_value=["mock_sidecar"],
        ), mock.patch(
            "paasta_tools.kubernetes_tools.load_system_paasta_config",
            autospec=True,
        ):
            if prometheus_port:
                self.deployment.config_dict["prometheus_port"] = prometheus_port
            mock_system_config = mock.Mock()
            mock_docker_volumes: Sequence[DockerVolume] = []
            mock_hacheck_sidecar_volumes: Sequence[DockerVolume] = []
            mock_aws_ebs_volumes: Sequence[AwsEbsVolume] = []
            mock_secret_volumes: Sequence[SecretVolume] = []
            ports = [V1ContainerPort(container_port=port) for port in expected_ports]
            expected = [
                V1Container(
                    args=mock_get_args.return_value,
                    command=mock_get_cmd.return_value,
                    env=mock_get_container_env.return_value,
                    resources=mock_get_resource_requirements.return_value,
                    image=mock_get_docker_url.return_value,
                    lifecycle=V1Lifecycle(
                        pre_stop=V1LifecycleHandler(
                            _exec=V1ExecAction(command=["/bin/sh", "-c", "sleep 30"])
                        )
                    ),
                    liveness_probe=V1Probe(
                        failure_threshold=30,
                        http_get=V1HTTPGetAction(
                            path="/status", port=8888, scheme="HTTP"
                        ),
                        initial_delay_seconds=60,
                        period_seconds=10,
                        timeout_seconds=10,
                    ),
                    readiness_probe=None,
                    name="fm",
                    ports=ports,
                    volume_mounts=mock_get_volume_mounts.return_value,
                    security_context=V1SecurityContext(
                        capabilities=V1Capabilities(drop=CAPS_DROP)
                    ),
                ),
                "mock_sidecar",
            ]
            service_namespace_config = mock.Mock()
            service_namespace_config.get_healthcheck_mode.return_value = "http"
            service_namespace_config.get_healthcheck_uri.return_value = "/status"
            service_namespace_config.get_longest_timeout_ms.return_value = 1000
            assert (
                self.deployment.get_kubernetes_containers(
                    docker_volumes=mock_docker_volumes,
                    hacheck_sidecar_volumes=mock_hacheck_sidecar_volumes,
                    system_paasta_config=mock_system_config,
                    aws_ebs_volumes=mock_aws_ebs_volumes,
                    secret_volumes=mock_secret_volumes,
                    service_namespace_config=service_namespace_config,
                )
                == expected
            )

    def test_get_liveness_probe(self):
        liveness_probe = V1Probe(
            failure_threshold=30,
            http_get=V1HTTPGetAction(path="/status", port=8888, scheme="HTTP"),
            initial_delay_seconds=60,
            period_seconds=10,
            timeout_seconds=10,
        )

        service_namespace_config = mock.Mock()
        service_namespace_config.get_healthcheck_mode.return_value = "http"
        service_namespace_config.get_healthcheck_uri.return_value = "/status"

        assert (
            self.deployment.get_liveness_probe(service_namespace_config)
            == liveness_probe
        )

    def test_get_liveness_probe_non_smartstack(self):
        service_namespace_config = mock.Mock()
        service_namespace_config.get_healthcheck_mode.return_value = None
        assert self.deployment.get_liveness_probe(service_namespace_config) is None

    def test_get_liveness_probe_numbers(self):
        liveness_probe = V1Probe(
            failure_threshold=1,
            http_get=V1HTTPGetAction(path="/status", port=8888, scheme="HTTP"),
            initial_delay_seconds=2,
            period_seconds=3,
            timeout_seconds=4,
        )

        service_namespace_config = mock.Mock()
        service_namespace_config.get_healthcheck_mode.return_value = "http"
        service_namespace_config.get_healthcheck_uri.return_value = "/status"

        self.deployment.config_dict["healthcheck_max_consecutive_failures"] = 1
        self.deployment.config_dict["healthcheck_grace_period_seconds"] = 2
        self.deployment.config_dict["healthcheck_interval_seconds"] = 3
        self.deployment.config_dict["healthcheck_timeout_seconds"] = 4

        assert (
            self.deployment.get_liveness_probe(service_namespace_config)
            == liveness_probe
        )

    def test_get_liveness_probe_tcp_socket(self):
        liveness_probe = V1Probe(
            failure_threshold=30,
            tcp_socket=V1TCPSocketAction(port=8888),
            initial_delay_seconds=60,
            period_seconds=10,
            timeout_seconds=10,
        )
        mock_service_namespace_config = mock.Mock()
        mock_service_namespace_config.get_healthcheck_mode.return_value = "tcp"
        assert (
            self.deployment.get_liveness_probe(mock_service_namespace_config)
            == liveness_probe
        )

    def test_get_liveness_probe_cmd(self):
        liveness_probe = V1Probe(
            failure_threshold=30,
            _exec=V1ExecAction(command=["/bin/sh", "-c", "/bin/true"]),
            initial_delay_seconds=60,
            period_seconds=10,
            timeout_seconds=10,
        )
        service_namespace_config = mock.Mock()
        service_namespace_config.get_healthcheck_mode.return_value = "cmd"
        self.deployment.config_dict["healthcheck_cmd"] = "/bin/true"
        assert (
            self.deployment.get_liveness_probe(service_namespace_config)
            == liveness_probe
        )

    def test_get_readiness_probe_in_mesh(self):
        service_namespace_config = mock.Mock()
        service_namespace_config.is_in_smartstack.return_value = True
        assert self.deployment.get_readiness_probe(service_namespace_config) is None

    @mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_liveness_probe",
        autospec=True,
    )
    def test_get_readiness_probe_not_in_mesh(self, mock_get_liveness_probe):
        service_namespace_config = mock.Mock()
        service_namespace_config.is_in_smartstack.return_value = False
        readiness_probe = self.deployment.get_readiness_probe(service_namespace_config)
        assert readiness_probe == mock_get_liveness_probe.return_value

    def test_get_security_context_without_cap_add(self):
        expected_security_context = V1SecurityContext(
            capabilities=V1Capabilities(drop=CAPS_DROP)
        )
        assert self.deployment.get_security_context() == expected_security_context

    def test_get_security_context_with_cap_add(self):
        self.deployment.config_dict["cap_add"] = ["SETGID"]
        expected_dropped_caps = sorted(list(set(CAPS_DROP) - {"SETGID"}))
        expected_security_context = V1SecurityContext(
            capabilities=V1Capabilities(add=["SETGID"], drop=expected_dropped_caps)
        )
        assert self.deployment.get_security_context() == expected_security_context

    def test_get_pod_volumes(self):
        mock_docker_volumes = [
            DockerVolume(hostPath="/nail/blah", containerPath="/nail/foo", mode="RO"),
            DockerVolume(hostPath="/nail/thing", containerPath="/nail/bar", mode="RO"),
        ]
        mock_hacheck_volumes = [
            DockerVolume(hostPath="/nail/blah", containerPath="/nail/foo", mode="RO"),
            DockerVolume(
                hostPath="/nail/anotherblah",
                containerPath="/nail/another-foo",
                mode="RO",
            ),
        ]
        mock_aws_ebs_volumes = [
            AwsEbsVolume(
                volume_id="vol-zzzzzzzzzzzzzzzzz",
                fs_type="ext4",
                container_path="/nail/qux",
                mode="RW",
                partition=123,
            )
        ]
        mock_secret_volumes = [
            SecretVolume(container_path="/nail/garply", secret_name="waldo"),
            SecretVolume(
                container_path="/nail/garply", secret_name="waldo", default_mode="0765"
            ),
            SecretVolume(
                container_path="/nail/garply",
                secret_name="waldo",
                items=[
                    SecretVolumeItem(key="aaa", mode="0567", path="bbb"),
                    SecretVolumeItem(key="ccc", path="ddd"),
                ],
            ),
        ]
        mock_projected_sa_volumes = [
            ProjectedSAVolume(
                container_path="/var/secret/something",
                audience="a.b.c",
                expiration_seconds=1234,
            )
        ]
        expected_volumes = [
            V1Volume(
                host_path=V1HostPathVolumeSource(path="/nail/blah"),
                name="host--slash-nailslash-blah",
            ),
            V1Volume(
                host_path=V1HostPathVolumeSource(path="/nail/thing"),
                name="host--slash-nailslash-thing",
            ),
            V1Volume(
                host_path=V1HostPathVolumeSource(path="/nail/anotherblah"),
                name="host--slash-nailslash-anotherblah",
            ),
            V1Volume(
                aws_elastic_block_store=V1AWSElasticBlockStoreVolumeSource(
                    volume_id="vol-zzzzzzzzzzzzzzzzz",
                    fs_type="ext4",
                    read_only=False,
                    partition=123,
                ),
                name="aws-ebs--vol-zzzzzzzzzzzzzzzzz123",
            ),
            V1Volume(
                name="secret--waldo",
                secret=V1SecretVolumeSource(
                    secret_name="paastasvc-kurupt-secret-kurupt-waldo", optional=False
                ),
            ),
            V1Volume(
                name="secret--waldo",
                secret=V1SecretVolumeSource(
                    secret_name="paastasvc-kurupt-secret-kurupt-waldo",
                    default_mode=0o765,
                    optional=False,
                ),
            ),
            V1Volume(
                name="secret--waldo",
                secret=V1SecretVolumeSource(
                    secret_name="paastasvc-kurupt-secret-kurupt-waldo",
                    items=[
                        V1KeyToPath(key="aaa", mode=0o567, path="bbb"),
                        V1KeyToPath(key="ccc", path="ddd"),
                    ],
                    optional=False,
                ),
            ),
            V1Volume(
                name="projected-sa--adot-bdot-c",
                projected=V1ProjectedVolumeSource(
                    sources=[
                        V1VolumeProjection(
                            service_account_token=V1ServiceAccountTokenProjection(
                                audience="a.b.c",
                                expiration_seconds=1234,
                                path="token",
                            )
                        ),
                    ],
                ),
            ),
        ]
        assert (
            self.deployment.get_pod_volumes(
                docker_volumes=mock_docker_volumes + mock_hacheck_volumes,
                aws_ebs_volumes=mock_aws_ebs_volumes,
                secret_volumes=mock_secret_volumes,
                projected_sa_volumes=mock_projected_sa_volumes,
            )
            == expected_volumes
        )

    def test_get_volume_mounts(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_sanitised_volume_name",
            autospec=True,
            return_value="some-volume",
        ):
            mock_docker_volumes = [
                DockerVolume(
                    hostPath="/nail/blah", containerPath="/nail/foo", mode="RO"
                ),
                DockerVolume(
                    hostPath="/nail/thing", containerPath="/nail/bar", mode="RW"
                ),
            ]
            mock_aws_ebs_volumes = [
                AwsEbsVolume(
                    volume_id="vol-ZZZZZZZZZZZZZZZZZ",
                    fs_type="ext4",
                    container_path="/nail/qux",
                    mode="RO",
                    partition=123,
                )
            ]
            mock_persistent_volumes = [
                PersistentVolume(
                    container_path="/blah", mode="RW", size=1, storage_class_name="foo"
                )
            ]
            mock_secret_volumes = [
                SecretVolume(container_path="/garply", secret_name="waldo")
            ]
            mock_projected_sa_volumes = [
                ProjectedSAVolume(
                    container_path="/var/secret/something",
                    audience="a.b.c",
                )
            ]
            expected_volumes = [
                V1VolumeMount(
                    mount_path="/nail/foo", name="some-volume", read_only=True
                ),
                V1VolumeMount(
                    mount_path="/nail/bar", name="some-volume", read_only=False
                ),
                V1VolumeMount(
                    mount_path="/nail/qux", name="some-volume", read_only=True
                ),
                V1VolumeMount(mount_path="/blah", name="some-volume", read_only=False),
                V1VolumeMount(mount_path="/garply", name="some-volume", read_only=True),
                V1VolumeMount(
                    mount_path="/var/secret/something",
                    name="some-volume",
                    read_only=True,
                ),
            ]
            assert (
                self.deployment.get_volume_mounts(
                    docker_volumes=mock_docker_volumes,
                    aws_ebs_volumes=mock_aws_ebs_volumes,
                    persistent_volumes=mock_persistent_volumes,
                    secret_volumes=mock_secret_volumes,
                    projected_sa_volumes=mock_projected_sa_volumes,
                )
                == expected_volumes
            )

    @pytest.mark.parametrize(
        "config_dict, service, instance, expected_secret_name, expected_signature_name",
        [
            (
                {"boto_keys": ["pew"]},
                "zuora_integration",
                "sync_ads_settings_post_budget_edit_batch_daemon",
                "paasta-boto-key-zuora--integration-sync--ads--settings--po-4xbg",
                "paastasvc-zuora--integration-secret-zuora--integration-paasta-boto-key-zuora--integration-sync--ads--settings--po-4xbg-signature",
            ),
            (
                {"boto_keys": ["few"]},
                "zuora_integration",
                "reprocess_zuora_amend_callouts_batch_daemon",
                "paasta-boto-key-zuora--integration-reprocess--zuora--amend-jztw",
                "paastasvc-zuora--integration-secret-zuora--integration-paasta-boto-key-zuora--integration-reprocess--zuora--amend-jztw-signature",
            ),
            (
                {
                    "boto_keys": ["foo"],
                },
                "kafka_discovery",
                "main",
                "paasta-boto-key-kafka--discovery-main",
                "paastasvc-kafka--discovery-secret-kafka--discovery-paasta-boto-key-kafka--discovery-main-signature",
            ),
            (
                {"boto_keys": ["pew"]},
                "yelp-main",
                "lives_data_action_content_ingester_worker",
                "paasta-boto-key-yelp-main-lives--data--action--content--in-4pxl",
                "paastasvc-yelp-main-secret-yelp-main-paasta-boto-key-yelp-main-lives--data--action--content--in-4pxl-signature",
            ),
            (
                {
                    "boto_keys": ["fuu"],
                    "namespace": "paastasvc-compute-infra-test-service",
                },
                "compute-infra-test-service",
                "boto_keys_test_1",
                "paasta-boto-key-compute-infra-test-service-boto--keys--test--1",
                "paastasvc-compute-infra-test-service-secret-compute-infra-test-service-paasta-boto-key-compute-infra-test-service-boto--keys--test--1-signature",
            ),
            ({}, "", "", "", ""),
        ],
    )
    def test_get_boto_volume(
        self,
        config_dict,
        service,
        instance,
        expected_secret_name,
        expected_signature_name,
    ):
        deployment = KubernetesDeploymentConfig(
            service=service,
            instance=instance,
            cluster="mega-cluster",
            config_dict=config_dict,
            branch_dict=None,
            soa_dir="/nail/blah",
        )

        with mock.patch(
            "paasta_tools.kubernetes_tools.get_secret_signature",
            return_value="hash",
            autospec=True,
        ) as get_signature:
            volumes = deployment.get_boto_volume()

        if config_dict:
            # check against existing signatures
            assert (
                get_signature.call_args[1]["signature_name"] == expected_signature_name
            )

            # check against existing secrets
            assert volumes.secret.secret_name == expected_secret_name

            assert len(volumes.secret.items) == len(config_dict["boto_keys"]) * 4
            for key in config_dict["boto_keys"]:
                assert any(
                    [item.path == f"{key}.yaml" for item in volumes.secret.items]
                )
        else:
            assert volumes is None

    @pytest.mark.parametrize(
        "config_dict, expected_secret_mount_items_count",
        [
            ({"datastore_credentials": {"mysql": ["credential1", "credential2"]}}, 2),
            ({"datastore_credentials": {"mysql": ["credential3"]}}, 1),
            (
                {
                    "datastore_credentials": {
                        "mysql": ["credential1"],
                        "cassandra": ["credential4", "credential5"],
                    }
                },
                3,
            ),
        ],
    )
    def test_get_datastore_credentials_secrets_volumes_exists(
        self, config_dict, expected_secret_mount_items_count
    ):
        deployment = KubernetesDeploymentConfig(
            service="my-service",
            instance="my-instance",
            cluster="mega-cluster",
            config_dict=config_dict,
            branch_dict=None,
            soa_dir="/nail/blah",
        )

        with mock.patch(
            "paasta_tools.kubernetes_tools.get_secret_signature",
            return_value="hash",
            autospec=True,
        ):
            secret_mount_items = (
                deployment.get_datastore_credentials_secrets_volume().secret.items
            )
            assert len(secret_mount_items) == expected_secret_mount_items_count

    @pytest.mark.parametrize(
        "config_dict",
        [
            {"datastore_credentials": {"mysql": []}},
            {"datastore_credentials": {"mysql": [], "cassandra": []}},
            {},
        ],
    )
    def test_get_datastore_credentials_secrets_volumes_not_exist(self, config_dict):
        deployment = KubernetesDeploymentConfig(
            service="my-service",
            instance="my-instance",
            cluster="mega-cluster",
            config_dict=config_dict,
            branch_dict=None,
            soa_dir="/nail/blah",
        )

        with mock.patch(
            "paasta_tools.kubernetes_tools.get_secret_signature",
            return_value="hash",
            autospec=True,
        ):
            if len(config_dict) == 0:
                assert deployment.get_datastore_credentials_secrets_volume() is None
            else:
                secret_items = (
                    deployment.get_datastore_credentials_secrets_volume().secret.items
                )
                assert len(secret_items) == 0

    @pytest.mark.parametrize(
        "config_dict",
        [
            {"crypto_keys": {"encrypt": ["mad"], "decrypt": ["max"]}},
            {"crypto_keys": {"decrypt": ["furiosa"]}},
            {},
        ],
    )
    def test_get_crypto_volume(self, config_dict):
        deployment = KubernetesDeploymentConfig(
            service="my-service",
            instance="my-instance",
            cluster="mega-cluster",
            config_dict=config_dict,
            branch_dict=None,
            soa_dir="/nail/blah",
        )
        with mock.patch.object(
            deployment, "get_crypto_secret_hash", return_value="hash"
        ):
            volumes = deployment.get_crypto_volume()
        if config_dict:
            (volumes.secret.secret_name == "paasta-crypto-key-my-service-my-instance")

            assert len(volumes.secret.items) == len(config_dict["crypto_keys"])
            assert set(deployment.get_crypto_keys_from_config()) == {
                item.path.rstrip(".json") for item in volumes.secret.items
            }
        else:
            assert volumes is None

    def test_get_sanitised_service_name(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_service",
            autospec=True,
            return_value="my_service",
        ):
            assert self.deployment.get_sanitised_service_name() == "my--service"

    def test_get_sanitised_instance_name(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_instance",
            autospec=True,
            return_value="my_instance",
        ):
            assert self.deployment.get_sanitised_instance_name() == "my--instance"

    def test_get_desired_instances(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_aws_ebs_volumes",
            autospec=True,
        ) as mock_get_aws_ebs_volumes:
            mock_get_aws_ebs_volumes.return_value = ["some-ebs-vol"]
            with pytest.raises(Exception):
                self.deployment.get_desired_instances()

    def test_format_kubernetes_job(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.get_git_sha_from_dockerurl",
            autospec=True,
        ) as mock_get_git_sha, mock.patch(
            "paasta_tools.kubernetes_tools.load_system_paasta_config",
            autospec=True,
        ) as mock_load_system_config, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_docker_url",
            autospec=True,
        ) as mock_get_docker_url, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_service",
            autospec=True,
            return_value="kurupt",
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_instance",
            autospec=True,
            return_value="fm",
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_image_version",
            autospec=True,
        ) as mock_get_image_version, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_pod_template_spec",
            autospec=True,
        ) as mock_get_pod_template_spec, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_kubernetes_metadata",
            autospec=True,
        ) as mock_get_kubernetes_metadata:
            mock_get_kubernetes_metadata.return_value.labels = {
                "paasta.yelp.com/owner": "whatever"
            }
            mock_get_pod_template_spec.return_value.metadata.labels = {}
            job = self.deployment.format_kubernetes_job("foobar", 100)
            assert job == V1Job(
                api_version="batch/v1",
                kind="Job",
                metadata=mock_get_kubernetes_metadata.return_value,
                spec=V1JobSpec(
                    active_deadline_seconds=100,
                    ttl_seconds_after_finished=0,
                    template=mock_get_pod_template_spec.return_value,
                ),
            )
            mock_get_git_sha.assert_called_once_with(
                mock_get_docker_url.return_value,
                long=True,
            )
            mock_get_kubernetes_metadata.assert_called_once_with(
                self.deployment,
                mock_get_git_sha.return_value,
            )
            mock_get_pod_template_spec.assert_called_once_with(
                self.deployment,
                git_sha=mock_get_git_sha.return_value,
                system_paasta_config=mock_load_system_config.return_value,
                restart_on_failure=False,
                include_sidecars=False,
                force_no_routable_ip=True,
                include_liveness_probe=False,
                include_readiness_probe=False,
            )
            assert job.metadata.labels == {
                "paasta.yelp.com/owner": "whatever",
                "paasta.yelp.com/image_version": mock_get_image_version.return_value,
                "paasta.yelp.com/job_type": "foobar",
            }
            assert mock_get_pod_template_spec.return_value.metadata.labels == {
                "paasta.yelp.com/image_version": mock_get_image_version.return_value,
                "paasta.yelp.com/job_type": "foobar",
            }

    @mock.patch(
        "paasta_tools.kubernetes_tools.get_git_sha_from_dockerurl", autospec=True
    )
    def test_format_kubernetes_app_dict(self, _):
        with mock.patch(
            "paasta_tools.kubernetes_tools.load_system_paasta_config", autospec=True
        ) as mock_load_system_config, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_docker_url",
            autospec=True,
        ) as mock_get_docker_url, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_service",
            autospec=True,
            return_value="kurupt",
        ) as mock_get_service, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_instance",
            autospec=True,
            return_value="fm",
        ) as mock_get_instance, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_desired_instances",
            autospec=True,
        ) as mock_get_instances, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_deployment_strategy_config",
            autospec=True,
        ) as mock_get_deployment_strategy_config, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_sanitised_volume_name",
            autospec=True,
        ), mock.patch(
            "paasta_tools.kubernetes_tools.get_config_hash", autospec=True
        ) as mock_get_config_hash, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_force_bounce",
            autospec=True,
        ) as mock_get_force_bounce, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.sanitize_for_config_hash",
            autospec=True,
        ) as mock_sanitize_for_config_hash, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_persistent_volumes",
            autospec=True,
        ) as mock_get_persistent_volumes, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_volume_claim_templates",
            autospec=True,
        ) as mock_get_volumes_claim_templates, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_pod_template_spec",
            autospec=True,
        ) as mock_get_pod_template_spec, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_kubernetes_metadata",
            autospec=True,
        ) as mock_get_kubernetes_metadata:
            mock_get_persistent_volumes.return_value = []
            ret = self.deployment.format_kubernetes_app()
            assert mock_load_system_config.called
            assert mock_get_docker_url.called
            mock_get_config_hash.assert_called_with(
                mock_sanitize_for_config_hash.return_value,
                force_bounce=mock_get_force_bounce.return_value,
            )
            expected = V1Deployment(
                api_version="apps/v1",
                kind="Deployment",
                metadata=mock_get_kubernetes_metadata.return_value,
                spec=V1DeploymentSpec(
                    min_ready_seconds=0,
                    replicas=mock_get_instances.return_value,
                    selector=V1LabelSelector(
                        match_labels={
                            "paasta.yelp.com/instance": mock_get_instance.return_value,
                            "paasta.yelp.com/service": mock_get_service.return_value,
                        }
                    ),
                    revision_history_limit=0,
                    strategy=mock_get_deployment_strategy_config.return_value,
                    template=mock_get_pod_template_spec.return_value,
                ),
            )
            assert ret == expected
            assert (
                mock.call(
                    "yelp.com/paasta_config_sha", mock_get_config_hash.return_value
                )
                in ret.metadata.labels.__setitem__.mock_calls
            )
            assert (
                mock.call(
                    "paasta.yelp.com/config_sha", mock_get_config_hash.return_value
                )
                in ret.metadata.labels.__setitem__.mock_calls
            )
            assert (
                mock.call(
                    "yelp.com/paasta_config_sha", mock_get_config_hash.return_value
                )
                in ret.spec.template.metadata.labels.__setitem__.mock_calls
            )
            assert (
                mock.call(
                    "paasta.yelp.com/config_sha", mock_get_config_hash.return_value
                )
                in ret.spec.template.metadata.labels.__setitem__.mock_calls
            )

            mock_get_deployment_strategy_config.side_effect = Exception(
                "Bad bounce method"
            )
            with pytest.raises(InvalidKubernetesConfig):
                self.deployment.format_kubernetes_app()

            mock_get_persistent_volumes.return_value = [mock.Mock()]
            ret = self.deployment.format_kubernetes_app()
            expected = V1StatefulSet(
                api_version="apps/v1",
                kind="StatefulSet",
                metadata=mock_get_kubernetes_metadata.return_value,
                spec=V1StatefulSetSpec(
                    service_name="kurupt-fm",
                    replicas=mock_get_instances.return_value,
                    selector=V1LabelSelector(
                        match_labels={
                            "paasta.yelp.com/instance": mock_get_instance.return_value,
                            "paasta.yelp.com/service": mock_get_service.return_value,
                        }
                    ),
                    revision_history_limit=0,
                    template=mock_get_pod_template_spec.return_value,
                    volume_claim_templates=mock_get_volumes_claim_templates.return_value,
                    pod_management_policy="OrderedReady",
                ),
            )
            assert ret == expected
            assert (
                mock.call(
                    "yelp.com/paasta_config_sha", mock_get_config_hash.return_value
                )
                in ret.metadata.labels.__setitem__.mock_calls
            )
            assert (
                mock.call(
                    "paasta.yelp.com/config_sha", mock_get_config_hash.return_value
                )
                in ret.metadata.labels.__setitem__.mock_calls
            )
            assert (
                mock.call(
                    "yelp.com/paasta_config_sha", mock_get_config_hash.return_value
                )
                in ret.spec.template.metadata.labels.__setitem__.mock_calls
            )
            assert (
                mock.call(
                    "paasta.yelp.com/config_sha", mock_get_config_hash.return_value
                )
                in ret.spec.template.metadata.labels.__setitem__.mock_calls
            )

    @mock.patch(
        "paasta_tools.kubernetes_tools.load_system_paasta_config",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_volumes",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_kubernetes_containers",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_pod_volumes",
        autospec=True,
        return_value=[],
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_node_affinity",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.load_service_namespace_config",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_termination_grace_period",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_pod_anti_affinity",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.create_pod_topology_spread_constraints",
        autospec=True,
    )
    @pytest.mark.parametrize(
        "autoscaling_metric_provider",
        [
            None,
            METRICS_PROVIDER_UWSGI,
            METRICS_PROVIDER_PISCINA,
            METRICS_PROVIDER_GUNICORN,
        ],
    )
    @pytest.mark.parametrize(
        "in_smtstk,routable_ip,node_affinity,anti_affinity,spec_affinity,termination_grace_period,pod_topology",
        [
            (True, "true", None, None, {}, None, []),
            (False, "false", None, None, {}, 10, []),
            # an node affinity absent but pod anti affinity present
            (
                False,
                "false",
                None,
                "pod_anti_affinity",
                {"affinity": V1Affinity(pod_anti_affinity="pod_anti_affinity")},
                None,
                [],
            ),
            # an affinity obj is only added if there is a node affinity
            (
                False,
                "false",
                "a_node_affinity",
                "pod_anti_affinity",
                {
                    "affinity": V1Affinity(
                        node_affinity="a_node_affinity",
                        pod_anti_affinity="pod_anti_affinity",
                    )
                },
                None,
                [],
            ),
        ],
    )
    def test_get_pod_template_spec(
        self,
        mock_create_pod_topology_spread_constraints,
        mock_get_pod_anti_affinity,
        mock_get_termination_grace_period,
        mock_load_service_namespace_config,
        mock_get_node_affinity,
        mock_get_pod_volumes,
        mock_get_kubernetes_containers,
        mock_get_volumes,
        mock_load_system_paasta_config,
        in_smtstk,
        routable_ip,
        pod_topology,
        node_affinity,
        anti_affinity,
        spec_affinity,
        termination_grace_period,
        autoscaling_metric_provider,
    ):
        mock_service_namespace_config = mock.Mock()
        mock_load_service_namespace_config.return_value = mock_service_namespace_config
        mock_service_namespace_config.is_in_smartstack.return_value = in_smtstk
        mock_get_node_affinity.return_value = node_affinity
        mock_get_pod_anti_affinity.return_value = anti_affinity
        mock_create_pod_topology_spread_constraints.return_value = pod_topology
        mock_system_paasta_config = mock.Mock()
        mock_system_paasta_config.get_kubernetes_add_registration_labels.return_value = (
            True
        )
        mock_system_paasta_config.get_topology_spread_constraints.return_value = []
        mock_system_paasta_config.get_pod_defaults.return_value = dict(dns_policy="foo")
        mock_load_system_paasta_config.return_value = mock_system_paasta_config
        mock_system_paasta_config.get_service_auth_token_volume_config.return_value = {}
        mock_get_termination_grace_period.return_value = termination_grace_period

        if autoscaling_metric_provider:
            mock_config_dict = KubernetesDeploymentConfigDict(
                min_instances=1,
                max_instances=3,
                autoscaling={
                    "metrics_providers": [{"type": autoscaling_metric_provider}]
                },
                deploy_group="fake_group",
            )
            autoscaled_deployment = KubernetesDeploymentConfig(
                service="kurupt",
                instance="fm",
                cluster="brentford",
                config_dict=mock_config_dict,
                branch_dict=None,
            )
            ret = autoscaled_deployment.get_pod_template_spec(
                git_sha="aaaa123", system_paasta_config=mock_system_paasta_config
            )
        else:
            ret = self.deployment.get_pod_template_spec(
                git_sha="aaaa123", system_paasta_config=mock_system_paasta_config
            )

        assert mock_load_service_namespace_config.called
        assert mock_service_namespace_config.is_in_smartstack.called
        assert mock_get_pod_volumes.called
        assert mock_get_volumes.called
        assert mock_load_system_paasta_config.called
        pod_spec_kwargs = dict(
            service_account_name=None,
            containers=mock_get_kubernetes_containers.return_value,
            share_process_namespace=True,
            node_selector={"yelp.com/pool": "default"},
            restart_policy="Always",
            volumes=[],
            dns_policy="foo",
            termination_grace_period_seconds=termination_grace_period,
        )
        pod_spec_kwargs.update(spec_affinity)

        expected_labels = {
            "paasta.yelp.com/pool": "default",
            "yelp.com/paasta_git_sha": "aaaa123",
            "yelp.com/paasta_instance": "fm",
            "yelp.com/paasta_service": "kurupt",
            "paasta.yelp.com/git_sha": "aaaa123",
            "paasta.yelp.com/instance": "fm",
            "paasta.yelp.com/service": "kurupt",
            "paasta.yelp.com/autoscaled": "true"
            if autoscaling_metric_provider
            else "false",
            "paasta.yelp.com/cluster": "brentford",
            "registrations.paasta.yelp.com/kurupt.fm": "true",
            "yelp.com/owner": "compute_infra_platform_experience",
            "paasta.yelp.com/managed": "true",
        }
        if in_smtstk:
            expected_labels["paasta.yelp.com/weight"] = "10"

        if autoscaling_metric_provider:
            expected_labels["paasta.yelp.com/deploy_group"] = "fake_group"
            if autoscaling_metric_provider != METRICS_PROVIDER_UWSGI:
                expected_labels[
                    f"paasta.yelp.com/scrape_{autoscaling_metric_provider}_prometheus"
                ] = "true"
        if autoscaling_metric_provider in (
            METRICS_PROVIDER_UWSGI,
            METRICS_PROVIDER_GUNICORN,
        ):
            routable_ip = "true"

        expected_annotations = {
            "smartstack_registrations": '["kurupt.fm"]',
            "paasta.yelp.com/routable_ip": routable_ip,
            "iam.amazonaws.com/role": "",
        }
        if autoscaling_metric_provider == METRICS_PROVIDER_UWSGI:
            expected_annotations["autoscaling"] = "uwsgi"

        expected = V1PodTemplateSpec(
            metadata=V1ObjectMeta(
                labels=expected_labels,
                annotations=expected_annotations,
            ),
            spec=V1PodSpec(**pod_spec_kwargs),
        )

        assert ret == expected

    @mock.patch(
        "paasta_tools.kubernetes_tools.load_system_paasta_config",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.load_service_namespace_config", autospec=True
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_kubernetes_containers",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_volumes",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_pod_volumes",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_node_affinity",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_termination_grace_period",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_pod_anti_affinity",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.create_pod_topology_spread_constraints",
        autospec=True,
    )
    @pytest.mark.parametrize(
        "metrics_providers,expected_labels",
        [
            # No metrics providers
            ([], {}),
            # Single providers
            ([METRICS_PROVIDER_UWSGI], {}),
            (
                [METRICS_PROVIDER_PISCINA],
                {"paasta.yelp.com/scrape_piscina_prometheus": "true"},
            ),
            (
                [METRICS_PROVIDER_GUNICORN],
                {"paasta.yelp.com/scrape_gunicorn_prometheus": "true"},
            ),
            ([METRICS_PROVIDER_CPU], {}),
            ([METRICS_PROVIDER_UWSGI_V2], {}),
            # Two provider combinations
            (
                [METRICS_PROVIDER_UWSGI, METRICS_PROVIDER_PISCINA],
                {"paasta.yelp.com/scrape_piscina_prometheus": "true"},
            ),
            (
                [METRICS_PROVIDER_UWSGI, METRICS_PROVIDER_GUNICORN],
                {"paasta.yelp.com/scrape_gunicorn_prometheus": "true"},
            ),
            (
                [METRICS_PROVIDER_PISCINA, METRICS_PROVIDER_GUNICORN],
                {
                    "paasta.yelp.com/scrape_piscina_prometheus": "true",
                    "paasta.yelp.com/scrape_gunicorn_prometheus": "true",
                },
            ),
            ([METRICS_PROVIDER_UWSGI, METRICS_PROVIDER_CPU], {}),
            (
                [METRICS_PROVIDER_PISCINA, METRICS_PROVIDER_CPU],
                {"paasta.yelp.com/scrape_piscina_prometheus": "true"},
            ),
            (
                [METRICS_PROVIDER_GUNICORN, METRICS_PROVIDER_CPU],
                {"paasta.yelp.com/scrape_gunicorn_prometheus": "true"},
            ),
            # Three provider combinations
            (
                [
                    METRICS_PROVIDER_UWSGI,
                    METRICS_PROVIDER_PISCINA,
                    METRICS_PROVIDER_GUNICORN,
                ],
                {
                    "paasta.yelp.com/scrape_piscina_prometheus": "true",
                    "paasta.yelp.com/scrape_gunicorn_prometheus": "true",
                },
            ),
            (
                [
                    METRICS_PROVIDER_UWSGI,
                    METRICS_PROVIDER_PISCINA,
                    METRICS_PROVIDER_CPU,
                ],
                {"paasta.yelp.com/scrape_piscina_prometheus": "true"},
            ),
            (
                [
                    METRICS_PROVIDER_UWSGI,
                    METRICS_PROVIDER_GUNICORN,
                    METRICS_PROVIDER_CPU,
                ],
                {"paasta.yelp.com/scrape_gunicorn_prometheus": "true"},
            ),
            (
                [
                    METRICS_PROVIDER_PISCINA,
                    METRICS_PROVIDER_GUNICORN,
                    METRICS_PROVIDER_CPU,
                ],
                {
                    "paasta.yelp.com/scrape_piscina_prometheus": "true",
                    "paasta.yelp.com/scrape_gunicorn_prometheus": "true",
                },
            ),
            # All providers
            (
                [
                    METRICS_PROVIDER_UWSGI,
                    METRICS_PROVIDER_PISCINA,
                    METRICS_PROVIDER_GUNICORN,
                    METRICS_PROVIDER_CPU,
                ],
                {
                    "paasta.yelp.com/scrape_piscina_prometheus": "true",
                    "paasta.yelp.com/scrape_gunicorn_prometheus": "true",
                },
            ),
            (
                [
                    METRICS_PROVIDER_UWSGI,
                    METRICS_PROVIDER_PISCINA,
                    METRICS_PROVIDER_GUNICORN,
                    METRICS_PROVIDER_CPU,
                    METRICS_PROVIDER_UWSGI_V2,
                ],
                {
                    "paasta.yelp.com/scrape_piscina_prometheus": "true",
                    "paasta.yelp.com/scrape_gunicorn_prometheus": "true",
                },
            ),
        ],
    )
    def test_get_pod_template_spec_multiple_metrics_providers(
        self,
        mock_create_pod_topology_spread_constraints,
        mock_get_pod_anti_affinity,
        mock_get_termination_grace_period,
        mock_load_service_namespace_config,
        mock_get_node_affinity,
        mock_get_pod_volumes,
        mock_get_kubernetes_containers,
        mock_get_volumes,
        mock_load_system_paasta_config,
        metrics_providers: List[str],
        expected_labels: Dict[str, str],
    ):
        """Test get_pod_template_spec with multiple metrics providers in all combinations."""
        mock_service_namespace_config = mock.Mock()
        mock_load_service_namespace_config.return_value = mock_service_namespace_config
        mock_service_namespace_config.is_in_smartstack.return_value = True
        mock_get_node_affinity.return_value = None
        mock_get_pod_anti_affinity.return_value = None
        mock_create_pod_topology_spread_constraints.return_value = []
        mock_get_termination_grace_period.return_value = None

        mock_system_paasta_config = mock.Mock()
        mock_system_paasta_config.get_kubernetes_add_registration_labels.return_value = (
            True
        )
        mock_system_paasta_config.get_topology_spread_constraints.return_value = []
        mock_system_paasta_config.get_pod_defaults.return_value = {"dns_policy": "foo"}
        mock_system_paasta_config.get_hacheck_sidecar_volumes.return_value = []
        mock_load_system_paasta_config.return_value = mock_system_paasta_config
        mock_system_paasta_config.get_service_auth_token_volume_config.return_value = {}

        # Deployment config with multiple metrics providers
        mock_config_dict = KubernetesDeploymentConfigDict(
            min_instances=1,
            max_instances=3,
            autoscaling={
                "metrics_providers": [
                    {"type": provider} for provider in metrics_providers
                ]
            },
            deploy_group="fake_group",
        )
        deployment = KubernetesDeploymentConfig(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            config_dict=mock_config_dict,
            branch_dict=None,
        )

        ret = deployment.get_pod_template_spec(
            git_sha="aaaa123", system_paasta_config=mock_system_paasta_config
        )

        assert mock_load_system_paasta_config.called

        # Each metric provider has its own set of labels. We expect to see all of them.
        actual_labels = ret.metadata.labels
        for expected_label, expected_value in expected_labels.items():
            assert actual_labels.get(expected_label) == expected_value, (
                f"Expected label {expected_label}={expected_value}, "
                f"but got {actual_labels.get(expected_label)}"
            )

        # Deploy group label is only set when specific metrics providers are used
        deploy_group_providers = {
            METRICS_PROVIDER_UWSGI,
            METRICS_PROVIDER_PISCINA,
            METRICS_PROVIDER_GUNICORN,
        }
        if any(provider in deploy_group_providers for provider in metrics_providers):
            assert actual_labels.get("paasta.yelp.com/deploy_group") == "fake_group"

        # Autoscaled label is always set
        assert actual_labels.get("paasta.yelp.com/autoscaled") == "true"

    @mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_prometheus_port",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.should_use_metrics_provider",
        autospec=True,
    )
    @pytest.mark.parametrize(
        "ip_configured,in_smtstk,prometheus_port,should_use_uwsgi_provider,should_use_gunicorn_provider,expected",
        [
            (False, True, 8888, False, False, "true"),
            (False, False, 8888, False, False, "true"),
            (False, True, None, False, False, "true"),
            (True, False, None, False, False, "true"),
            (False, False, None, True, False, "true"),
            (False, False, None, False, False, "false"),
            (False, False, None, False, True, "true"),
        ],
    )
    def test_routable_ip(
        self,
        mock_should_use_metrics_provider,
        mock_get_prometheus_port,
        ip_configured,
        in_smtstk,
        prometheus_port,
        should_use_uwsgi_provider,
        should_use_gunicorn_provider,
        expected,
    ):
        def mock_should_use_metrics_provider_fn(p: str) -> bool:
            if p == METRICS_PROVIDER_UWSGI:
                return should_use_uwsgi_provider
            elif p == METRICS_PROVIDER_GUNICORN:
                return should_use_gunicorn_provider
            return False

        mock_get_prometheus_port.return_value = prometheus_port
        self.deployment.should_use_metrics_provider = mock_should_use_metrics_provider_fn  # type: ignore
        mock_service_namespace_config = mock.Mock()
        mock_service_namespace_config.is_in_smartstack.return_value = in_smtstk
        mock_system_paasta_config = mock.Mock()

        self.deployment.config_dict["routable_ip"] = ip_configured
        ret = self.deployment.has_routable_ip(
            mock_service_namespace_config, mock_system_paasta_config
        )

        assert ret == expected

    def test_create_pod_topology_spread_constraints(self):
        configured_constraints: List[TopologySpreadConstraintDict] = [
            {
                "topology_key": "kubernetes.io/hostname",
                "max_skew": 1,
                "when_unsatisfiable": "ScheduleAnyway",
            },
            {
                "topology_key": "topology.kubernetes.io/zone",
                "max_skew": 3,
                "when_unsatisfiable": "DoNotSchedule",
            },
        ]

        expected_constraints = [
            V1TopologySpreadConstraint(
                label_selector=V1LabelSelector(
                    match_labels={
                        "paasta.yelp.com/service": "schematizer",
                        "paasta.yelp.com/instance": "main",
                    }
                ),
                max_skew=1,
                topology_key="kubernetes.io/hostname",
                when_unsatisfiable="ScheduleAnyway",
            ),
            V1TopologySpreadConstraint(
                label_selector=V1LabelSelector(
                    match_labels={
                        "paasta.yelp.com/service": "schematizer",
                        "paasta.yelp.com/instance": "main",
                    }
                ),
                max_skew=3,
                topology_key="topology.kubernetes.io/zone",
                when_unsatisfiable="DoNotSchedule",
            ),
        ]

        assert (
            kubernetes_tools.create_pod_topology_spread_constraints(
                "schematizer", "main", configured_constraints
            )
            == expected_constraints
        )

    @pytest.mark.parametrize(
        "raw_selectors,expected",
        [
            ({}, {"yelp.com/pool": "default"}),  # no node_selectors case
            (  # node_selectors configs case, simple items become k8s selectors
                {
                    "select_key": "select_value",
                    "affinity_key": {"operator": "In", "values": ["affinity_value"]},
                },
                {"yelp.com/pool": "default", "select_key": "select_value"},
            ),
        ],
    )
    def test_get_node_selectors(self, raw_selectors, expected):
        if raw_selectors:
            self.deployment.config_dict["node_selectors"] = raw_selectors
        assert self.deployment.get_node_selector() == expected

    def test_get_node_affinity_with_reqs(self):
        deployment = KubernetesDeploymentConfig(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            config_dict={
                "deploy_whitelist": ["habitat", ["habitat_a"]],
                "node_selectors": {
                    "instance_type": ["a1.1xlarge"],
                },
            },
            branch_dict=None,
            soa_dir="/nail/blah",
        )

        assert deployment.get_node_affinity() == V1NodeAffinity(
            required_during_scheduling_ignored_during_execution=V1NodeSelector(
                node_selector_terms=[
                    V1NodeSelectorTerm(
                        match_expressions=[
                            V1NodeSelectorRequirement(
                                key="yelp.com/habitat",
                                operator="In",
                                values=["habitat_a"],
                            ),
                            V1NodeSelectorRequirement(
                                key="node.kubernetes.io/instance-type",
                                operator="In",
                                values=["a1.1xlarge"],
                            ),
                        ]
                    )
                ],
            ),
        )

    def test_get_node_affinity_no_reqs(self):
        deployment = KubernetesDeploymentConfig(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            config_dict={},
            branch_dict=None,
            soa_dir="/nail/blah",
        )

        assert deployment.get_node_affinity() is None

    def test_get_node_affinity_with_preferences(self):
        deployment = KubernetesDeploymentConfig(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            config_dict={
                "deploy_whitelist": ["habitat", ["habitat_a"]],
                "node_selectors_preferred": [
                    {
                        "weight": 1,
                        "preferences": {
                            "instance_type": ["a1.1xlarge"],
                        },
                    }
                ],
            },
            branch_dict=None,
            soa_dir="/nail/blah",
        )

        assert deployment.get_node_affinity() == V1NodeAffinity(
            required_during_scheduling_ignored_during_execution=V1NodeSelector(
                node_selector_terms=[
                    V1NodeSelectorTerm(
                        match_expressions=[
                            V1NodeSelectorRequirement(
                                key="yelp.com/habitat",
                                operator="In",
                                values=["habitat_a"],
                            ),
                        ]
                    )
                ],
            ),
            preferred_during_scheduling_ignored_during_execution=[
                V1PreferredSchedulingTerm(
                    weight=1,
                    preference=V1NodeSelectorTerm(
                        match_expressions=[
                            V1NodeSelectorRequirement(
                                key="node.kubernetes.io/instance-type",
                                operator="In",
                                values=["a1.1xlarge"],
                            ),
                        ]
                    ),
                )
            ],
        )

    def test_get_node_affinity_no_reqs_with_global_override(self):
        """
        Given global node affinity overrides and no deployment specific requirements, the globals should be used
        """
        assert self.deployment.get_node_affinity(
            {"default": {"topology.kubernetes.io/zone": ["us-west-1a", "us-west-1b"]}},
        ) == V1NodeAffinity(
            required_during_scheduling_ignored_during_execution=V1NodeSelector(
                node_selector_terms=[
                    V1NodeSelectorTerm(
                        match_expressions=[
                            V1NodeSelectorRequirement(
                                key="topology.kubernetes.io/zone",
                                operator="In",
                                values=["us-west-1a", "us-west-1b"],
                            )
                        ]
                    )
                ],
            ),
        )

    def test_get_node_affinity_no_reqs_with_global_override_and_deployment_config(self):
        """
        Given global node affinity overrides and deployment specific requirements, globals should be ignored
        """
        deployment = KubernetesDeploymentConfig(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            config_dict={
                "node_selectors": {"topology.kubernetes.io/zone": ["us-west-1a"]},
                "node_selectors_preferred": [
                    {
                        "weight": 1,
                        "preferences": {
                            "instance_type": ["a1.1xlarge"],
                        },
                    }
                ],
            },
            branch_dict=None,
            soa_dir="/nail/blah",
        )
        actual = deployment.get_node_affinity(
            {"default": {"topology.kubernetes.io/zone": ["us-west-1a", "us-west-1b"]}},
        )
        expected = V1NodeAffinity(
            required_during_scheduling_ignored_during_execution=V1NodeSelector(
                node_selector_terms=[
                    V1NodeSelectorTerm(
                        match_expressions=[
                            V1NodeSelectorRequirement(
                                key="topology.kubernetes.io/zone",
                                operator="In",
                                values=["us-west-1a"],
                            ),
                        ]
                    )
                ],
            ),
            preferred_during_scheduling_ignored_during_execution=[
                V1PreferredSchedulingTerm(
                    weight=1,
                    preference=V1NodeSelectorTerm(
                        match_expressions=[
                            V1NodeSelectorRequirement(
                                key="node.kubernetes.io/instance-type",
                                operator="In",
                                values=["a1.1xlarge"],
                            ),
                        ]
                    ),
                )
            ],
        )
        assert actual == expected

    def test_get_node_affinity_no_reqs_with_global_override_and_deployment_config_habitat(
        self,
    ):
        """
        Given global node affinity overrides and deployment specific zone selector, globals should be ignored
        """
        deployment = KubernetesDeploymentConfig(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            config_dict={"node_selectors": {"yelp.com/habitat": ["uswest1astagef"]}},
            branch_dict=None,
            soa_dir="/nail/blah",
        )
        actual = deployment.get_node_affinity(
            {"default": {"topology.kubernetes.io/zone": ["us-west-1a", "us-west-1b"]}},
        )
        expected = V1NodeAffinity(
            required_during_scheduling_ignored_during_execution=V1NodeSelector(
                node_selector_terms=[
                    V1NodeSelectorTerm(
                        match_expressions=[
                            V1NodeSelectorRequirement(
                                key="yelp.com/habitat",
                                operator="In",
                                values=["uswest1astagef"],
                            ),
                        ]
                    )
                ],
            )
        )
        assert actual == expected

    @pytest.mark.parametrize(
        "anti_affinity,expected",
        [
            (None, None),  # no anti-affinity case
            ([], None),  # empty anti-affinity
            (  # single anti-affinity
                KubeAffinityCondition(service="s1", instance="i1"),
                [{"paasta.yelp.com/service": "s1", "paasta.yelp.com/instance": "i1"}],
            ),
            (  # multiple anti-affinity case
                [
                    KubeAffinityCondition(service="s1", instance="i1"),
                    KubeAffinityCondition(instance="i2"),
                    KubeAffinityCondition(service="s3"),
                ],
                [
                    {"paasta.yelp.com/service": "s1", "paasta.yelp.com/instance": "i1"},
                    {"paasta.yelp.com/instance": "i2"},
                    {"paasta.yelp.com/service": "s3"},
                ],
            ),
        ],
    )
    def test_get_pod_anti_affinity(self, anti_affinity, expected):
        self.deployment.config_dict["anti_affinity"] = anti_affinity
        expected_affinity = None
        if expected:
            terms = [
                V1PodAffinityTerm(
                    topology_key="kubernetes.io/hostname",
                    label_selector=V1LabelSelector(match_labels=selector),
                )
                for selector in expected
            ]
            expected_affinity = V1PodAntiAffinity(
                required_during_scheduling_ignored_during_execution=terms
            )
        assert self.deployment.get_pod_anti_affinity() == expected_affinity

    @pytest.mark.parametrize(
        "is_in_smartstack,termination_action,expected",
        [
            (True, None, ["/bin/sh", "-c", "sleep 30"]),  # no termination action
            (True, "", ["/bin/sh", "-c", "sleep 30"]),  # empty termination action
            (True, [], ["/bin/sh", "-c", "sleep 30"]),  # empty termination action
            (True, "/bin/no-args", ["/bin/no-args"]),  # no args command
            (True, ["/bin/bash", "cmd.sh"], ["/bin/bash", "cmd.sh"]),  # no args command
            (
                False,
                None,
                ["/bin/sh", "-c", "sleep 0"],
            ),  # no termination action and not in smartstack
        ],
    )
    def test_kubernetes_container_termination_action(
        self, is_in_smartstack, termination_action, expected
    ):
        mock_service_namespace_config = mock.Mock()
        mock_service_namespace_config.is_in_smartstack.return_value = is_in_smartstack
        mock_service_namespace_config.get_longest_timeout_ms.return_value = 1000

        if termination_action:
            self.deployment.config_dict["lifecycle"] = {
                "pre_stop_command": termination_action
            }
        handler = V1LifecycleHandler(_exec=V1ExecAction(command=expected))
        assert (
            self.deployment.get_kubernetes_container_termination_action(
                mock_service_namespace_config
            )
            == handler
        )

    @pytest.mark.parametrize(
        "whitelist,blacklist,expected",
        [
            (None, [], []),  # no whitelist/blacklist case
            (  # whitelist only case
                ("habitat", ["habitat_a", "habitat_b"]),
                [],
                [("yelp.com/habitat", "In", ["habitat_a", "habitat_b"])],
            ),
            (  # blacklist only case
                None,
                [("habitat", "habitat_a"), ("habitat", "habitat_b")],
                [
                    ("yelp.com/habitat", "NotIn", ["habitat_a"]),
                    ("yelp.com/habitat", "NotIn", ["habitat_b"]),
                ],
            ),
            (  # whitelist and blacklist case
                ("habitat", ["habitat_a", "habitat_b"]),
                [("region", "region_a"), ("habitat", "habitat_c")],
                [
                    ("yelp.com/habitat", "In", ["habitat_a", "habitat_b"]),
                    ("yelp.com/region", "NotIn", ["region_a"]),
                    ("yelp.com/habitat", "NotIn", ["habitat_c"]),
                ],
            ),
        ],
    )
    def test_whitelist_blacklist_to_requirements(self, whitelist, blacklist, expected):
        assert (
            allowlist_denylist_to_requirements(allowlist=whitelist, denylist=blacklist)
            == expected
        )

    @pytest.mark.parametrize(
        "node_selectors,expected",
        [
            ({}, []),  # no node_selectors case
            (  # node_selectors config case, complex items become requirements
                {
                    "select_key": "select_value",  # simple item, excluded
                    "implicit_in_key": ["implicit_value"],  # shorthand "In" case
                    "a_key": [
                        {"operator": "In", "values": ["a_value"]},
                        {"operator": "NotIn", "values": ["a_value"]},
                        {"operator": "Exists"},
                        {"operator": "DoesNotExist"},
                        {"operator": "Gt", "value": 100},
                        {"operator": "Lt", "value": 200},
                    ],
                },
                [
                    ("implicit_in_key", "In", ["implicit_value"]),
                    ("a_key", "In", ["a_value"]),
                    ("a_key", "NotIn", ["a_value"]),
                    ("a_key", "Exists", []),
                    ("a_key", "DoesNotExist", []),
                    ("a_key", "Gt", ["100"]),
                    ("a_key", "Lt", ["200"]),
                ],
            ),
        ],
    )
    def test_raw_selectors_to_requirements(self, node_selectors, expected):
        assert raw_selectors_to_requirements(node_selectors) == expected

    def test_raw_selectors_to_requirements_error(self):
        node_selectors = {
            "error_key": [{"operator": "BadOperator"}],  # type: ignore
        }
        with pytest.raises(ValueError):
            raw_selectors_to_requirements(node_selectors)  # type: ignore

    @pytest.mark.parametrize(
        "is_autoscaled, autoscaled_label",
        (
            (True, "true"),
            (False, "false"),
        ),
    )
    def test_get_kubernetes_metadata(self, is_autoscaled, autoscaled_label):
        with mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_service",
            autospec=True,
            return_value="kurupt",
        ) as mock_get_service, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_instance",
            autospec=True,
            return_value="fm",
        ) as mock_get_instance, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.is_autoscaling_enabled",
            autospec=True,
            return_value=is_autoscaled,
        ):
            ret = self.deployment.get_kubernetes_metadata("aaa123")
            assert ret == V1ObjectMeta(
                labels={
                    "yelp.com/paasta_git_sha": "aaa123",
                    "yelp.com/paasta_instance": mock_get_instance.return_value,
                    "yelp.com/paasta_service": mock_get_service.return_value,
                    "paasta.yelp.com/git_sha": "aaa123",
                    "paasta.yelp.com/instance": mock_get_instance.return_value,
                    "paasta.yelp.com/service": mock_get_service.return_value,
                    "paasta.yelp.com/autoscaled": autoscaled_label,
                    "paasta.yelp.com/pool": "default",
                    "paasta.yelp.com/cluster": "brentford",
                    "yelp.com/owner": "compute_infra_platform_experience",
                    "paasta.yelp.com/managed": "true",
                },
                name="kurupt-fm",
                namespace="paastasvc-kurupt",
            )

    @pytest.mark.parametrize(
        "metrics_provider",
        (METRICS_PROVIDER_CPU,),
    )
    def test_get_autoscaling_metric_spec_cpu(self, metrics_provider):
        # with cpu
        config_dict = KubernetesDeploymentConfigDict(
            {
                "min_instances": 1,
                "max_instances": 3,
                "autoscaling": {
                    "metrics_providers": [{"type": metrics_provider, "setpoint": 0.5}]
                },
            }
        )
        mock_config = KubernetesDeploymentConfig(  # type: ignore
            service="service",
            cluster="cluster",
            instance="instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        return_value = KubernetesDeploymentConfig.get_autoscaling_metric_spec(
            mock_config,
            "fake_name",
            "cluster",
            KubeClient(),
            "paasta",
        )
        annotations: Dict[Any, Any] = {}
        expected_res = V2HorizontalPodAutoscaler(
            kind="HorizontalPodAutoscaler",
            metadata=V1ObjectMeta(
                name="fake_name",
                namespace="paasta",
                annotations=annotations,
                labels=mock.ANY,
            ),
            spec=V2HorizontalPodAutoscalerSpec(
                behavior=mock_config.get_autoscaling_scaling_policy(
                    autoscaling_params={},
                    max_replicas=3,
                ),
                max_replicas=3,
                min_replicas=1,
                metrics=[
                    V2MetricSpec(
                        type="Resource",
                        resource=V2ResourceMetricSource(
                            name="cpu",
                            target=V2MetricTarget(
                                type="Utilization",
                                average_utilization=50.0,
                            ),
                        ),
                    )
                ],
                scale_target_ref=V2CrossVersionObjectReference(
                    api_version="apps/v1",
                    kind="Deployment",
                    name="fake_name",
                ),
            ),
        )
        assert expected_res == return_value

    @mock.patch(
        "paasta_tools.kubernetes_tools.load_system_paasta_config",
        autospec=True,
        return_value=mock.Mock(
            get_legacy_autoscaling_signalflow=lambda: "fake_signalflow_query"
        ),
    )
    def test_get_autoscaling_metric_spec_uwsgi_prometheus(
        self, fake_system_paasta_config
    ):
        config_dict = KubernetesDeploymentConfigDict(
            {
                "min_instances": 1,
                "max_instances": 3,
                "autoscaling": {
                    "metrics_providers": [
                        {
                            "type": METRICS_PROVIDER_UWSGI,
                            "setpoint": 0.4,
                            "forecast_policy": "moving_average",
                            "moving_average_window_seconds": 300,
                        }
                    ]
                },
            }
        )
        mock_config = KubernetesDeploymentConfig(  # type: ignore
            service="service",
            cluster="cluster",
            instance="instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        return_value = KubernetesDeploymentConfig.get_autoscaling_metric_spec(
            mock_config,
            "fake_name",
            "cluster",
            KubeClient(),
            "paasta",
        )
        expected_res = V2HorizontalPodAutoscaler(
            kind="HorizontalPodAutoscaler",
            metadata=V1ObjectMeta(
                name="fake_name",
                namespace="paasta",
                annotations={},
                labels=mock.ANY,
            ),
            spec=V2HorizontalPodAutoscalerSpec(
                behavior=mock_config.get_autoscaling_scaling_policy(
                    autoscaling_params={},
                    max_replicas=3,
                ),
                max_replicas=3,
                min_replicas=1,
                metrics=[
                    V2MetricSpec(
                        type="Object",
                        object=V2ObjectMetricSource(
                            metric=V2MetricIdentifier(
                                name="service-instance-uwsgi-prom",
                            ),
                            target=V2MetricTarget(
                                type="Value",
                                value=1,
                            ),
                            described_object=V2CrossVersionObjectReference(
                                api_version="apps/v1",
                                kind="Deployment",
                                name="fake_name",
                            ),
                        ),
                    ),
                ],
                scale_target_ref=V2CrossVersionObjectReference(
                    api_version="apps/v1",
                    kind="Deployment",
                    name="fake_name",
                ),
            ),
        )

        assert expected_res == return_value

    @mock.patch(
        "paasta_tools.kubernetes_tools.load_system_paasta_config",
        autospec=True,
        return_value=mock.Mock(
            get_legacy_autoscaling_signalflow=lambda: "fake_signalflow_query"
        ),
    )
    def test_get_autoscaling_metric_spec_uwsgi_v2_prometheus(
        self, fake_system_paasta_config
    ):
        config_dict = KubernetesDeploymentConfigDict(
            {
                "min_instances": 1,
                "max_instances": 3,
                "autoscaling": {
                    "metrics_providers": [
                        {
                            "type": METRICS_PROVIDER_UWSGI_V2,
                            "setpoint": 0.4,
                            "forecast_policy": "moving_average",
                            "moving_average_window_seconds": 300,
                        }
                    ]
                },
            }
        )
        mock_config = KubernetesDeploymentConfig(  # type: ignore
            service="service",
            cluster="cluster",
            instance="instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        return_value = KubernetesDeploymentConfig.get_autoscaling_metric_spec(
            mock_config,
            "fake_name",
            "cluster",
            KubeClient(),
            "paasta",
        )
        expected_res = V2HorizontalPodAutoscaler(
            kind="HorizontalPodAutoscaler",
            metadata=V1ObjectMeta(
                name="fake_name",
                namespace="paasta",
                annotations={},
                labels=mock.ANY,
            ),
            spec=V2HorizontalPodAutoscalerSpec(
                behavior=mock_config.get_autoscaling_scaling_policy(
                    autoscaling_params={},
                    max_replicas=3,
                ),
                max_replicas=3,
                min_replicas=1,
                metrics=[
                    V2MetricSpec(
                        type="Object",
                        object=V2ObjectMetricSource(
                            metric=V2MetricIdentifier(
                                name="service-instance-uwsgi-v2-prom",
                            ),
                            target=V2MetricTarget(
                                type="AverageValue",
                                average_value=0.4,
                            ),
                            described_object=V2CrossVersionObjectReference(
                                api_version="apps/v1",
                                kind="Deployment",
                                name="fake_name",
                            ),
                        ),
                    ),
                ],
                scale_target_ref=V2CrossVersionObjectReference(
                    api_version="apps/v1",
                    kind="Deployment",
                    name="fake_name",
                ),
            ),
        )

        assert expected_res == return_value

    @mock.patch(
        "paasta_tools.kubernetes_tools.load_system_paasta_config",
        autospec=True,
        return_value=mock.Mock(
            get_legacy_autoscaling_signalflow=lambda: "fake_signalflow_query"
        ),
    )
    def test_get_autoscaling_metric_spec_gunicorn_prometheus(
        self, fake_system_paasta_config
    ):
        config_dict = KubernetesDeploymentConfigDict(
            {
                "min_instances": 1,
                "max_instances": 3,
                "autoscaling": {
                    "metrics_providers": [
                        {
                            "type": METRICS_PROVIDER_GUNICORN,
                            "setpoint": 0.5,
                            "forecast_policy": "moving_average",
                            "moving_average_window_seconds": 300,
                        }
                    ]
                },
            }
        )
        mock_config = KubernetesDeploymentConfig(  # type: ignore
            service="service",
            cluster="cluster",
            instance="instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        return_value = KubernetesDeploymentConfig.get_autoscaling_metric_spec(
            mock_config,
            "fake_name",
            "cluster",
            KubeClient(),
            "paasta",
        )
        expected_res = V2HorizontalPodAutoscaler(
            kind="HorizontalPodAutoscaler",
            metadata=V1ObjectMeta(
                name="fake_name",
                namespace="paasta",
                annotations={},
                labels=mock.ANY,
            ),
            spec=V2HorizontalPodAutoscalerSpec(
                behavior=mock_config.get_autoscaling_scaling_policy(
                    autoscaling_params={},
                    max_replicas=3,
                ),
                max_replicas=3,
                min_replicas=1,
                metrics=[
                    V2MetricSpec(
                        type="Object",
                        object=V2ObjectMetricSource(
                            metric=V2MetricIdentifier(
                                name="service-instance-gunicorn-prom",
                            ),
                            target=V2MetricTarget(
                                type="Value",
                                value=1,
                            ),
                            described_object=V2CrossVersionObjectReference(
                                api_version="apps/v1",
                                kind="Deployment",
                                name="fake_name",
                            ),
                        ),
                    ),
                ],
                scale_target_ref=V2CrossVersionObjectReference(
                    api_version="apps/v1",
                    kind="Deployment",
                    name="fake_name",
                ),
            ),
        )

        assert expected_res == return_value

    def test_override_scaledown_policies(self):
        config_dict = KubernetesDeploymentConfigDict(
            {
                "min_instances": 1,
                "max_instances": 3,
                "autoscaling": {
                    "scaledown_policies": {
                        "stabilizationWindowSeconds": 123,
                        "policies": [
                            {"type": "Percent", "value": 45, "periodSeconds": 67}
                        ],
                    }
                },
            }
        )
        mock_config = KubernetesDeploymentConfig(  # type: ignore
            service="service",
            cluster="cluster",
            instance="instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        hpa = KubernetesDeploymentConfig.get_autoscaling_metric_spec(
            mock_config,
            "fake_name",
            "cluster",
            KubeClient(),
            "paasta",
        )
        assert hpa.spec.behavior["scaleDown"] == {
            "stabilizationWindowSeconds": 123,
            "selectPolicy": "Max",
            "policies": [{"type": "Percent", "value": 45, "periodSeconds": 67}],
        }

    def test_get_autoscaling_metric_spec_bespoke(self):
        config_dict = KubernetesDeploymentConfigDict(
            {
                "min_instances": 1,
                "max_instances": 3,
                "autoscaling": {
                    "metrics_providers": [
                        {"decision_policy": "bespoke", "setpoint": 0.5}
                    ]
                },
            }
        )
        mock_config = KubernetesDeploymentConfig(  # type: ignore
            service="service",
            cluster="cluster",
            instance="instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        return_value = KubernetesDeploymentConfig.get_autoscaling_metric_spec(
            mock_config,
            "fake_name",
            "cluster",
            KubeClient(),
            "paasta",
        )
        expected_res = None
        assert expected_res == return_value

    @mock.patch(
        "paasta_tools.kubernetes_tools.get_kubernetes_secret_hashes",
        autospec=True,
    )
    @pytest.mark.parametrize(
        "app_type,app_spec_type",
        [
            (V1Deployment, V1DeploymentSpec),
            (
                V1StatefulSet,
                functools.partial(V1StatefulSetSpec, service_name="fake_service_name"),
            ),
        ],
    )
    def test_sanitize_for_config_hash(
        self,
        mock_get_kubernetes_secret_hashes,
        app_type,
        app_spec_type,
    ):
        def make_deployment_config():
            return app_type(
                metadata=V1ObjectMeta(name="qwe", labels={"mc": "grindah"}),
                spec=app_spec_type(
                    replicas=2,
                    selector=V1LabelSelector(match_labels={"freq": "108.9"}),
                    template=V1PodTemplateSpec(
                        spec=V1PodSpec(
                            containers=[
                                V1Container(name="fake_container", env=[]),
                            ],
                        ),
                    ),
                ),
            )

        mock_config = make_deployment_config()
        mock_config_with_soa_sha = make_deployment_config()
        mock_config_with_soa_sha.spec.template.spec.containers[0].env.append(
            V1EnvVar(name="PAASTA_SOA_CONFIGS_SHA", value="fake_soa_git_sha"),
        )

        no_sha_ret = self.deployment.sanitize_for_config_hash(mock_config)
        with_sha_ret = self.deployment.sanitize_for_config_hash(
            mock_config_with_soa_sha
        )

        assert "replicas" not in no_sha_ret["spec"].keys()
        assert (
            no_sha_ret["paasta_secrets"]
            == mock_get_kubernetes_secret_hashes.return_value
        )
        assert (
            len(with_sha_ret["spec"]["template"]["spec"]["containers"][0]["env"]) == 0
        )
        # this means that with or without a SOA sha env var, the config SHA will
        # not be affected. if this is no longer true, this will cause a big bounce.
        assert no_sha_ret == with_sha_ret

    def test_get_kubernetes_secret_env_vars(self):
        assert self.deployment.get_kubernetes_secret_env_vars(
            secret_env_vars={"SOME": "SECRET(a_ref)"},
            shared_secret_env_vars={"A": "SHAREDSECRET(_ref1)"},
        ) == [
            V1EnvVar(
                name="SOME",
                value_from=V1EnvVarSource(
                    secret_key_ref=V1SecretKeySelector(
                        name="paastasvc-kurupt-secret-kurupt-a--ref",
                        key="a_ref",
                        optional=False,
                    )
                ),
            ),
            V1EnvVar(
                name="A",
                value_from=V1EnvVarSource(
                    secret_key_ref=V1SecretKeySelector(
                        name="paastasvc-kurupt-secret-underscore-shared-underscore-ref1",
                        key="_ref1",
                        optional=False,
                    )
                ),
            ),
        ]

    def test_get_bounce_margin_factor(self):
        assert isinstance(self.deployment.get_bounce_margin_factor(), float)

    def test_get_bounce_margin_factor_specific_value(self):
        self.deployment.config_dict["bounce_margin_factor"] = 0.345
        assert self.deployment.get_bounce_margin_factor() == 0.345

    def test_get_bounce_margin_factor_default(self):
        assert self.deployment.get_bounce_margin_factor() == 0.95

    def test_get_volume_claim_templates(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_persistent_volumes",
            autospec=True,
        ) as mock_get_persistent_volumes, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_persistent_volume_name",
            autospec=True,
        ) as mock_get_persistent_volume_name, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_storage_class_name",
            autospec=True,
        ) as mock_get_storage_class_name:
            mock_get_persistent_volumes.return_value = [{"size": 20}, {"size": 10}]
            expected = [
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(
                        name=mock_get_persistent_volume_name.return_value
                    ),
                    spec=V1PersistentVolumeClaimSpec(
                        access_modes=["ReadWriteOnce"],
                        storage_class_name=mock_get_storage_class_name.return_value,
                        resources=V1ResourceRequirements(requests={"storage": "10Gi"}),
                    ),
                ),
                V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(
                        name=mock_get_persistent_volume_name.return_value
                    ),
                    spec=V1PersistentVolumeClaimSpec(
                        access_modes=["ReadWriteOnce"],
                        storage_class_name=mock_get_storage_class_name.return_value,
                        resources=V1ResourceRequirements(requests={"storage": "20Gi"}),
                    ),
                ),
            ]
            ret = self.deployment.get_volume_claim_templates()
            assert expected[0] in ret
            assert expected[1] in ret
            assert len(ret) == 2

    def test_get_storage_class_name_default(self):
        pv = kubernetes_tools.PersistentVolume()  # type: ignore  # We will need to make this TypedDict non-total to remove this comment
        assert self.deployment.get_storage_class_name(pv) == "ebs"

    def test_get_storage_class_name_wrong(self):
        fake_sc = "fake_sc"
        pv = PersistentVolume(
            storage_class_name=fake_sc,
            size=1000,
            container_path="/dev/null",
            mode="rw",
        )
        assert self.deployment.get_storage_class_name(pv) == "ebs"

    @pytest.mark.parametrize("storage_class_name", ["ebs", "ebs-slow", "ebs-retain"])
    def test_get_storage_class_name_correct(self, storage_class_name):
        with mock.patch(
            "paasta_tools.kubernetes_tools.load_system_paasta_config", autospec=True
        ) as mock_load_system_config:
            mock_load_system_config.side_effect = None
            mock_load_system_config.return_value = mock.Mock(
                get_supported_storage_classes=mock.Mock(
                    return_value=["ebs", "ebs-slow", "ebs-retain"]
                ),
            )
            pv = PersistentVolume(
                storage_class_name=storage_class_name,
                size=1000,
                container_path="/dev/null",
                mode="rw",
            )
            assert self.deployment.get_storage_class_name(pv) == storage_class_name

    def test_get_persistent_volume_name(self):
        pv_name = self.deployment.get_persistent_volume_name(
            PersistentVolume(
                container_path="/blah/what", mode="ro", size=1, storage_class_name="foo"
            )
        )
        assert pv_name == "pv--slash-blahslash-what"


def test_get_kubernetes_services_running_here():
    with mock.patch(
        "paasta_tools.kubernetes_tools.requests.get", autospec=True
    ) as mock_requests_get:
        mock_requests_get.return_value.json.return_value = {"items": []}
        assert get_kubernetes_services_running_here() == []

        spec = {
            "containers": [
                {"name": "something-something", "ports": [{"containerPort": 8888}]}
            ]
        }

        mock_pod_results: dict = {
            "items": [
                # valid pod
                {
                    "status": {"phase": "Running", "podIP": "10.1.1.3"},
                    "metadata": {
                        "namespace": "paasta",
                        "labels": {
                            "yelp.com/paasta_service": "kurupt",
                            "yelp.com/paasta_instance": "fm",
                            "paasta.yelp.com/service": "kurupt",
                            "paasta.yelp.com/instance": "fm",
                        },
                        "annotations": {
                            "smartstack_registrations": "[]",
                            "iam.amazonaws.com/role": "",
                        },
                    },
                    "spec": spec,
                },
                # non-Running pod
                {
                    "status": {"phase": "Something", "podIP": "10.1.1.2"},
                    "metadata": {
                        "namespace": "paasta",
                        "labels": {
                            "yelp.com/paasta_service": "kurupt",
                            "yelp.com/paasta_instance": "garage",
                            "paasta.yelp.com/service": "kurupt",
                            "paasta.yelp.com/instance": "garage",
                        },
                        "annotations": {
                            "smartstack_registrations": "[]",
                            "iam.amazonaws.com/role": "",
                        },
                    },
                    "spec": spec,
                },
                # no pod IP
                {
                    "status": {"phase": "Running"},
                    "metadata": {
                        "namespace": "paasta",
                        "labels": {
                            "yelp.com/paasta_service": "kurupt",
                            "yelp.com/paasta_instance": "grindah",
                            "paasta.yelp.com/service": "kurupt",
                            "paasta.yelp.com/instance": "grindah",
                        },
                        "annotations": {
                            "smartstack_registrations": "[]",
                            "iam.amazonaws.com/role": "",
                        },
                    },
                    "spec": spec,
                },
                # no registration annotation
                {
                    "status": {"phase": "Running", "podIP": "10.1.1.1"},
                    "metadata": {
                        "namespace": "paasta",
                        "labels": {
                            "yelp.com/paasta_service": "kurupt",
                            "yelp.com/paasta_instance": "beats",
                            "paasta.yelp.com/service": "kurupt",
                            "paasta.yelp.com/instance": "beats",
                        },
                        "annotations": {"iam.amazonaws.com/role": ""},
                    },
                    "spec": spec,
                },
            ]
        }
        mock_requests_get.return_value.json.return_value = mock_pod_results

        assert get_kubernetes_services_running_here() == [
            KubernetesServiceRegistration(
                name="kurupt",
                instance="fm",
                port=8888,
                pod_ip="10.1.1.3",
                registrations=[],
                weight=10,
            )
        ]

        # mock a terminating pod
        mock_pod_results["items"][0]["metadata"]["deletionTimestamp"] = "now"
        mock_requests_get.return_value.json.return_value = mock_pod_results
        assert get_kubernetes_services_running_here(exclude_terminating=True) == []

        # if the kubelet is down we don't want to reconfigure nerve until it comes back
        # and we can be sure what is running or not
        mock_requests_get.side_effect = ConnectionError
        with pytest.raises(ConnectionError):
            get_kubernetes_services_running_here()


class MockNerveDict(dict):
    def is_in_smartstack(self):
        return False if self["name"] == "garage" else True


def test_get_kubernetes_services_running_here_for_nerve():
    with mock.patch(
        "paasta_tools.kubernetes_tools.load_system_paasta_config", autospec=True
    ) as mock_load_system_config, mock.patch(
        "paasta_tools.kubernetes_tools.get_kubernetes_services_running_here",
        autospec=True,
    ) as mock_get_kubernetes_services_running_here, mock.patch(
        "paasta_tools.kubernetes_tools.load_service_namespace_config", autospec=True
    ) as mock_load_service_namespace:
        mock_load_service_namespace.side_effect = (
            lambda service, namespace, soa_dir: MockNerveDict(name=namespace)
        )
        mock_get_kubernetes_services_running_here.return_value = [
            KubernetesServiceRegistration(
                name="kurupt",
                instance="fm",
                port=8888,
                pod_ip="10.1.1.1",
                registrations=["kurupt.fm"],
                weight=10,
            ),
            KubernetesServiceRegistration(
                name="unkurupt",
                instance="garage",
                port=8888,
                pod_ip="10.1.1.1",
                registrations=["unkurupt.garage"],
                weight=10,
            ),
            KubernetesServiceRegistration(
                name="kurupt",
                instance="garage",
                port=8888,
                pod_ip="10.1.1.1",
                registrations=[],
                weight=10,
            ),
        ]

        mock_load_system_config.side_effect = None
        mock_load_system_config.return_value = mock.Mock(
            get_cluster=mock.Mock(return_value="brentford"),
            get_register_k8s_pods=mock.Mock(return_value=False),
        )
        ret = get_kubernetes_services_running_here_for_nerve("brentford", "/nail/blah")
        assert ret == []

        mock_load_system_config.return_value = mock.Mock(
            get_cluster=mock.Mock(return_value="brentford"),
            get_register_k8s_pods=mock.Mock(return_value=True),
        )
        ret = get_kubernetes_services_running_here_for_nerve("brentford", "/nail/blah")
        assert ret == [
            (
                "kurupt.fm",
                {
                    "name": "fm",
                    "hacheck_ip": "10.1.1.1",
                    "service_ip": "10.1.1.1",
                    "port": 8888,
                    "weight": 10,
                },
            )
        ]

        mock_load_system_config.return_value = mock.Mock(
            get_cluster=mock.Mock(return_value="brentford"),
            get_register_k8s_pods=mock.Mock(return_value=True),
            get_kubernetes_use_hacheck_sidecar=mock.Mock(return_value=False),
        )
        ret = get_kubernetes_services_running_here_for_nerve("brentford", "/nail/blah")
        assert ret == [
            (
                "kurupt.fm",
                {
                    "name": "fm",
                    "service_ip": "10.1.1.1",
                    "port": 8888,
                    "extra_healthcheck_headers": {"X-Nerve-Check-IP": "10.1.1.1"},
                    "weight": 10,
                },
            )
        ]

        def mock_load_namespace_side(service, namespace, soa_dir):
            if namespace != "kurupt":
                raise Exception
            return MockNerveDict(name=namespace)

        mock_load_service_namespace.side_effect = mock_load_namespace_side
        ret = get_kubernetes_services_running_here_for_nerve("brentford", "/nail/blah")
        assert ret == []


def test_KubeClient():
    with mock.patch(
        "paasta_tools.kubernetes_tools.kube_config.load_kube_config", autospec=True
    ), mock.patch(
        "paasta_tools.kubernetes_tools.kube_client", autospec=True
    ) as mock_kube_client:
        client = KubeClient()
        assert client.deployments == mock_kube_client.AppsV1Api()
        assert client.core == mock_kube_client.CoreV1Api()


def test_ensure_namespace_doesnt_create_if_namespace_exists():
    with mock.patch(
        "paasta_tools.kubernetes_tools.ensure_paasta_api_rolebinding", autospec=True
    ), mock.patch(
        "paasta_tools.kubernetes_tools.ensure_paasta_namespace_limits", autospec=True
    ):
        mock_metadata = mock.Mock()
        type(mock_metadata).name = "paasta"
        mock_namespaces = mock.Mock(items=[mock.Mock(metadata=mock_metadata)])
        mock_client = mock.Mock(
            core=mock.Mock(list_namespace=mock.Mock(return_value=mock_namespaces)),
        )
        ensure_namespace(mock_client, namespace="paasta")
        assert not mock_client.core.create_namespace.called


def test_ensure_namespace_kube_system():
    with mock.patch(
        "paasta_tools.kubernetes_tools.ensure_paasta_api_rolebinding", autospec=True
    ), mock.patch(
        "paasta_tools.kubernetes_tools.ensure_paasta_namespace_limits", autospec=True
    ):
        mock_metadata = mock.Mock()
        type(mock_metadata).name = "kube-system"
        mock_namespaces = mock.Mock(items=[mock.Mock(metadata=mock_metadata)])
        mock_client = mock.Mock(
            core=mock.Mock(list_namespace=mock.Mock(return_value=mock_namespaces)),
        )
        ensure_namespace(mock_client, namespace="paasta")
        assert mock_client.core.create_namespace.called


def test_ensure_namespace_creates_namespace_if_doesnt_exist():
    with mock.patch(
        "paasta_tools.kubernetes_tools.ensure_paasta_api_rolebinding", autospec=True
    ), mock.patch(
        "paasta_tools.kubernetes_tools.ensure_paasta_namespace_limits", autospec=True
    ):
        mock_namespaces = mock.Mock(items=[])
        mock_client = mock.Mock(
            core=mock.Mock(list_namespace=mock.Mock(return_value=mock_namespaces)),
        )
        ensure_namespace(mock_client, namespace="paasta")
        assert mock_client.core.create_namespace.called


def test_ensure_paasta_api_rolebinding_creates_if_not_exist():
    mock_rolebindings = mock.Mock(items=[])
    mock_client = mock.Mock(
        rbac=mock.Mock(
            list_namespaced_role_binding=mock.Mock(return_value=mock_rolebindings)
        ),
    )

    ensure_paasta_api_rolebinding(mock_client, namespace="paastasvc-cool-service-name")
    assert mock_client.rbac.create_namespaced_role_binding.called


def test_ensure_paasta_api_rolebinding_doesnt_create_if_exists():
    mock_metadata = mock.Mock()
    type(mock_metadata).name = "paasta-api-server-per-namespace"
    mock_rolebindings = mock.Mock(items=[mock.Mock(metadata=mock_metadata)])
    mock_client = mock.Mock(
        rbac=mock.Mock(
            list_namespaced_role_binding=mock.Mock(return_value=mock_rolebindings)
        ),
    )

    ensure_paasta_api_rolebinding(mock_client, namespace="paastasvc-cool-service-name")
    assert not mock_client.rbac.create_namespaced_role_binding.called


def test_ensure_paasta_namespace_limits_creates_if_not_exist():
    mock_limits = mock.Mock(items=[])
    mock_client = mock.Mock(
        core=mock.Mock(list_namespaced_limit_range=mock.Mock(return_value=mock_limits)),
    )

    ensure_paasta_namespace_limits(mock_client, namespace="paastasvc-cool-service-name")
    assert mock_client.core.create_namespaced_limit_range.called


def test_ensure_paasta_namespace_limits_doesnt_create_if_exists():
    mock_metadata = mock.Mock()
    type(mock_metadata).name = "limit-mem-cpu-disk-per-container"
    mock_limits = mock.Mock(items=[mock.Mock(metadata=mock_metadata)])
    mock_client = mock.Mock(
        core=mock.Mock(list_namespaced_limit_range=mock.Mock(return_value=mock_limits)),
    )

    ensure_paasta_namespace_limits(mock_client, namespace="paastasvc-cool-service-name")
    assert not mock_client.core.create_namespaced_role_binding.called


@pytest.mark.parametrize(
    "addl_labels,replicas",
    (
        ({}, 3),
        ({"paasta.yelp.com/autoscaled": "false"}, 3),
        ({"paasta.yelp.com/autoscaled": "true"}, None),
    ),
)
def test_list_all_paasta_deployments(addl_labels, replicas):
    mock_deployments = mock.Mock(items=[])
    mock_stateful_sets = mock.Mock(items=[])
    mock_client = mock.Mock(
        deployments=mock.Mock(
            list_deployment_for_all_namespaces=mock.Mock(return_value=mock_deployments),
            list_stateful_set_for_all_namespaces=mock.Mock(
                return_value=mock_stateful_sets
            ),
        )
    )

    assert list_all_paasta_deployments(kube_client=mock_client) == []
    mock_items = [
        mock.Mock(
            metadata=mock.Mock(
                labels={
                    "yelp.com/paasta_service": "kurupt",
                    "yelp.com/paasta_instance": "fm",
                    "yelp.com/paasta_git_sha": "a12345",
                    "yelp.com/paasta_config_sha": "b12345",
                    "paasta.yelp.com/service": "kurupt",
                    "paasta.yelp.com/instance": "fm",
                    "paasta.yelp.com/git_sha": "a12345",
                    "paasta.yelp.com/config_sha": "b12345",
                    **addl_labels,
                },
                namespace="paasta",
            )
        ),
        mock.Mock(
            metadata=mock.Mock(
                labels={
                    "yelp.com/paasta_service": "kurupt",
                    "yelp.com/paasta_instance": "am",
                    "yelp.com/paasta_git_sha": "a12345",
                    "yelp.com/paasta_config_sha": "b12345",
                    "paasta.yelp.com/service": "kurupt",
                    "paasta.yelp.com/instance": "am",
                    "paasta.yelp.com/git_sha": "a12345",
                    "paasta.yelp.com/config_sha": "b12345",
                    **addl_labels,
                },
                namespace="test",
            )
        ),
    ]

    # Setting the number of replicas this way since spec
    # is a reserved argument for Mocks
    type(mock_items[0]).spec = mock.Mock(**{"replicas": replicas})
    type(mock_items[1]).spec = mock.Mock(**{"replicas": replicas})
    mock_deployments = mock.Mock(items=[mock_items[0]])
    mock_stateful_sets = mock.Mock(items=[mock_items[1]])
    mock_client = mock.Mock(
        deployments=mock.Mock(
            list_deployment_for_all_namespaces=mock.Mock(return_value=mock_deployments),
            list_stateful_set_for_all_namespaces=mock.Mock(
                return_value=mock_stateful_sets
            ),
        )
    )
    assert list_all_paasta_deployments(mock_client) == [
        KubeDeployment(
            service="kurupt",
            instance="fm",
            git_sha="a12345",
            namespace="paasta",
            image_version=None,
            config_sha="b12345",
            replicas=replicas,
        ),
        KubeDeployment(
            service="kurupt",
            instance="am",
            git_sha="a12345",
            namespace="test",
            image_version=None,
            config_sha="b12345",
            replicas=replicas,
        ),
    ]


@pytest.mark.parametrize(
    "addl_labels,replicas",
    (
        ({}, 3),
        ({"paasta.yelp.com/autoscaled": "false"}, 3),
        ({"paasta.yelp.com/autoscaled": "true"}, None),
    ),
)
def test_list_all_deployments(addl_labels, replicas):
    mock_deployments = mock.Mock(items=[])
    mock_stateful_sets = mock.Mock(items=[])
    mock_client = mock.Mock(
        deployments=mock.Mock(
            list_namespaced_deployment=mock.Mock(return_value=mock_deployments),
            list_namespaced_stateful_set=mock.Mock(return_value=mock_stateful_sets),
        )
    )
    assert list_all_deployments(kube_client=mock_client, namespace="paasta") == []

    mock_items = [
        mock.Mock(
            metadata=mock.Mock(
                labels={
                    "yelp.com/paasta_service": "kurupt",
                    "yelp.com/paasta_instance": "fm",
                    "yelp.com/paasta_git_sha": "a12345",
                    "yelp.com/paasta_config_sha": "b12345",
                    "paasta.yelp.com/service": "kurupt",
                    "paasta.yelp.com/instance": "fm",
                    "paasta.yelp.com/git_sha": "a12345",
                    "paasta.yelp.com/config_sha": "b12345",
                    **addl_labels,
                },
                namespace="paasta",
            )
        ),
        mock.Mock(
            metadata=mock.Mock(
                labels={
                    "yelp.com/paasta_service": "kurupt",
                    "yelp.com/paasta_instance": "am",
                    "yelp.com/paasta_git_sha": "a12345",
                    "yelp.com/paasta_config_sha": "b12345",
                    "paasta.yelp.com/service": "kurupt",
                    "paasta.yelp.com/instance": "am",
                    "paasta.yelp.com/git_sha": "a12345",
                    "paasta.yelp.com/config_sha": "b12345",
                    **addl_labels,
                },
                namespace="paasta",
            )
        ),
    ]

    # Setting the number of replicas this way since spec
    # is a reserved argument for Mocks
    type(mock_items[0]).spec = mock.Mock(**{"replicas": replicas})
    type(mock_items[1]).spec = mock.Mock(**{"replicas": replicas})
    mock_deployments = mock.Mock(items=[mock_items[0]])
    mock_stateful_sets = mock.Mock(items=[mock_items[1]])
    mock_client = mock.Mock(
        deployments=mock.Mock(
            list_namespaced_deployment=mock.Mock(return_value=mock_deployments),
            list_namespaced_stateful_set=mock.Mock(return_value=mock_stateful_sets),
        )
    )
    assert list_all_deployments(mock_client, namespace="paasta") == [
        KubeDeployment(
            service="kurupt",
            instance="fm",
            git_sha="a12345",
            namespace="paasta",
            image_version=None,
            config_sha="b12345",
            replicas=replicas,
        ),
        KubeDeployment(
            service="kurupt",
            instance="am",
            git_sha="a12345",
            namespace="paasta",
            image_version=None,
            config_sha="b12345",
            replicas=replicas,
        ),
    ]


@pytest.mark.parametrize(
    "pod_logs,container_name,term_error,expected",
    [
        (  # normal case: stdout read, container state error read
            "a_line\nnext_line\n",
            "my--container",
            mock.Mock(message="term_error"),
            {
                "stdout": ["a_line", "next_line", ""],
                "stderr": [],
                "error_message": "term_error",
            },
        ),
        (  # no-error case: just stdout read
            "a_line\nnext_line\n",
            "my--container",
            None,
            {"stdout": ["a_line", "next_line", ""], "stderr": [], "error_message": ""},
        ),
        (  # exc case, container state error takes precedent
            ApiException(http_resp=mock.MagicMock(data='{"message": "exc_error"}')),
            "my--container",
            mock.Mock(message="term_error"),
            {
                "stdout": [],
                "stderr": [],
                "error_message": "couldn't read stdout/stderr: 'term_error'",
            },
        ),
        (  # exc case, no container state error, so exc error used
            ApiException(http_resp=mock.MagicMock(data='{"message": "exc_error"}')),
            "my--container",
            mock.Mock(message=None),
            {
                "stdout": [],
                "stderr": [],
                "error_message": "couldn't read stdout/stderr: 'exc_error'",
            },
        ),
    ],
)
def test_get_tail_lines_for_kubernetes_container(
    event_loop,
    pod_logs,
    container_name,
    term_error,
    expected,
):
    kube_client = mock.MagicMock()
    kube_client.core.read_namespaced_pod_log.side_effect = [pod_logs]
    container = mock.MagicMock()
    container.name = container_name
    container.state.running = None
    container.state.waiting = None
    container.state.terminated = term_error
    pod = mock.MagicMock()
    pod.metadata.name = "my--pod"
    pod.metadata.namespace = "my_namespace"

    tail_lines = event_loop.run_until_complete(
        kubernetes_tools.get_tail_lines_for_kubernetes_container(
            kube_client=kube_client,
            pod=pod,
            container=container,
            num_tail_lines=10,
        ),
    )

    assert tail_lines == expected
    assert kube_client.core.read_namespaced_pod_log.call_args_list == [
        mock.call(
            name="my--pod",
            namespace="my_namespace",
            container=container_name,
            tail_lines=10,
            previous=False,
        ),
    ]


@pytest.mark.parametrize("messages_num", [3, 0])
def test_get_pod_event_messages(messages_num, event_loop):
    pod = mock.MagicMock()
    pod.metadata.name = "my--pod"
    pod.metadata.namespace = "my_namespace"

    events = []
    for i in range(messages_num):
        event = mock.MagicMock()
        event.message = f"message_{i}"
        event.last_timestamp = i
        events.append(event)

    kube_client = mock.MagicMock()

    with asynctest.patch(
        "paasta_tools.kubernetes_tools.get_events_for_object", autospec=True
    ) as mock_get_events_for_object:
        mock_get_events_for_object.return_value = events
        pod_event_messages = event_loop.run_until_complete(
            kubernetes_tools.get_pod_event_messages(kube_client=kube_client, pod=pod)
        )

    assert len(pod_event_messages) == messages_num
    if messages_num == 3:
        assert {"message": "message_0", "timeStamp": "0"} in pod_event_messages
        assert {"message": "message_1", "timeStamp": "1"} in pod_event_messages
        assert {"message": "message_2", "timeStamp": "2"} in pod_event_messages


def test_format_pod_event_messages():
    pod_event_messages = [
        {"message": "message_1", "time_stamp": "1"},
        {"message": "message_2", "time_stamp": "2"},
    ]
    pod_name = "test_pod"
    rows = kubernetes_tools.format_pod_event_messages(pod_event_messages, pod_name)

    assert rows[1] == "    Event at 1: message_1"
    assert rows[2] == "    Event at 2: message_2"


@given(integers(min_value=0), floats(min_value=0, max_value=1.0))
def test_max_unavailable(instances, bmf):
    res = max_unavailable(instances, bmf)
    if instances == 0:
        assert res == 0
    if instances > 0:
        assert res >= 1 and res <= instances
    assert type(res) is int


def test_pod_disruption_budget_for_service_instance():
    mock_namespace = "paasta"
    x = pod_disruption_budget_for_service_instance(
        service="foo_1",
        instance="bar_1",
        max_unavailable="10%",
        namespace=mock_namespace,
    )

    assert x.metadata.name == "foo--1-bar--1"
    assert x.metadata.namespace == "paasta"
    assert x.spec.max_unavailable == "10%"
    assert x.spec.selector.match_labels == {
        "paasta.yelp.com/service": "foo_1",
        "paasta.yelp.com/instance": "bar_1",
    }


def test_create_pod_disruption_budget():
    mock_client = mock.Mock()
    mock_pdr = V1PodDisruptionBudget()
    mock_namespace = "paasta"
    create_pod_disruption_budget(mock_client, mock_pdr, mock_namespace)
    mock_client.policy.create_namespaced_pod_disruption_budget.assert_called_with(
        namespace="paasta", body=mock_pdr
    )


def test_create_deployment():
    mock_client = mock.Mock()
    mock_namespace = "paasta"
    create_deployment(mock_client, V1Deployment(api_version="some"), mock_namespace)
    mock_client.deployments.create_namespaced_deployment.assert_called_with(
        namespace="paasta", body=V1Deployment(api_version="some")
    )


def test_update_deployment():
    mock_client = mock.Mock()
    mock_namespace = "paasta"
    update_deployment(
        mock_client,
        V1Deployment(metadata=V1ObjectMeta(name="kurupt")),
        mock_namespace,
    )
    mock_client.deployments.replace_namespaced_deployment.assert_called_with(
        namespace="paasta",
        name="kurupt",
        body=V1Deployment(metadata=V1ObjectMeta(name="kurupt")),
    )

    mock_client = mock.Mock()
    create_deployment(mock_client, V1Deployment(api_version="some"), mock_namespace)
    mock_client.deployments.create_namespaced_deployment.assert_called_with(
        namespace="paasta", body=V1Deployment(api_version="some")
    )


@mock.patch("paasta_tools.kubernetes_tools.KubernetesDeploymentConfig", autospec=True)
def test_set_instances_for_kubernetes_service_deployment(mock_kube_deploy_config):
    replicas = 5
    mock_client = mock.Mock()
    mock_kube_deploy_config.get_sanitised_deployment_name.return_value = (
        "fake_deployment"
    )
    mock_kube_deploy_config.get_persistent_volumes.return_value = False
    mock_kube_deploy_config.format_kubernetes_app.return_value = mock.Mock()
    set_instances_for_kubernetes_service(mock_client, mock_kube_deploy_config, replicas)
    assert mock_client.deployments.patch_namespaced_deployment_scale.call_count == 1


@mock.patch("paasta_tools.kubernetes_tools.KubernetesDeploymentConfig", autospec=True)
def test_set_instances_for_kubernetes_service_statefulset(mock_kube_deploy_config):
    replicas = 5
    mock_client = mock.Mock()
    mock_kube_deploy_config.get_sanitised_deployment_name.return_value = (
        "fake_stateful_set"
    )
    mock_kube_deploy_config.get_persistent_volumes.return_value = True
    mock_kube_deploy_config.format_kubernetes_app.return_value = mock.Mock()
    set_instances_for_kubernetes_service(mock_client, mock_kube_deploy_config, replicas)
    assert mock_client.deployments.patch_namespaced_stateful_set_scale.call_count == 1


@pytest.mark.parametrize(
    "has_persistent_volumes, expected_annotations",
    [
        (
            True,
            {"I-am": "stateful_set"},
        ),
        (
            False,
            {"I-am": "deployment"},
        ),
    ],
)
@mock.patch("paasta_tools.kubernetes_tools.KubernetesDeploymentConfig", autospec=True)
def test_get_annotations_for_kubernetes_service(
    mock_kube_deploy_config, has_persistent_volumes, expected_annotations
):
    mock_client = mock.Mock()
    mock_client.deployments.read_namespaced_stateful_set.return_value = V1StatefulSet(
        metadata=V1ObjectMeta(annotations={"I-am": "stateful_set"})
    )
    mock_client.deployments.read_namespaced_deployment.return_value = V1Deployment(
        metadata=V1ObjectMeta(annotations={"I-am": "deployment"})
    )
    mock_kube_deploy_config.get_sanitised_deployment_name.return_value = (
        "fake_k8s_service"
    )
    mock_kube_deploy_config.get_persistent_volumes.return_value = has_persistent_volumes
    mock_kube_deploy_config.format_kubernetes_app.return_value = mock.Mock()
    annotations = get_annotations_for_kubernetes_service(
        mock_client, mock_kube_deploy_config
    )
    assert annotations == expected_annotations


def test_create_custom_resource():
    mock_client = mock.Mock()
    formatted_resource = mock.Mock()
    create_custom_resource(
        kube_client=mock_client,
        formatted_resource=formatted_resource,
        version="v1",
        kind=mock.Mock(plural="someclusters"),
        group="yelp.com",
    )
    mock_client.custom.create_namespaced_custom_object.assert_called_with(
        namespace="paasta-someclusters",
        body=formatted_resource,
        version="v1",
        plural="someclusters",
        group="yelp.com",
    )


def test_update_custom_resource():
    mock_get_object = mock.Mock(return_value={"metadata": {"resourceVersion": 2}})
    mock_client = mock.Mock(
        custom=mock.Mock(get_namespaced_custom_object=mock_get_object)
    )
    mock_formatted_resource: Dict[Any, Any] = {"metadata": {}}
    update_custom_resource(
        kube_client=mock_client,
        formatted_resource=mock_formatted_resource,
        version="v1",
        kind=mock.Mock(plural="someclusters"),
        name="grindah",
        group="yelp.com",
    )
    mock_client.custom.replace_namespaced_custom_object.assert_called_with(
        namespace="paasta-someclusters",
        group="yelp.com",
        name="grindah",
        version="v1",
        plural="someclusters",
        body={"metadata": {"resourceVersion": 2}},
    )


def test_list_custom_resources():
    mock_list_object = mock.Mock(
        return_value={
            "items": [
                {"some": "nonpaasta"},
                {
                    "kind": "somecluster",
                    "metadata": {
                        "labels": {
                            "yelp.com/paasta_service": "kurupt",
                            "yelp.com/paasta_instance": "fm",
                            "yelp.com/paasta_config_sha": "con123",
                            "paasta.yelp.com/service": "kurupt",
                            "paasta.yelp.com/instance": "fm",
                            "paasta.yelp.com/config_sha": "con123",
                            "paasta.yelp.com/git_sha": "git123",
                        },
                        "name": "foo",
                        "namespace": "bar",
                    },
                },
            ]
        }
    )

    mock_client = mock.Mock(
        custom=mock.Mock(list_namespaced_custom_object=mock_list_object)
    )
    expected = [
        KubeCustomResource(
            service="kurupt",
            instance="fm",
            config_sha="con123",
            git_sha="git123",
            kind="somecluster",
            name="foo",
            namespace="bar",
        )
    ]
    assert (
        list_custom_resources(
            kind=mock.Mock(plural="someclusters"),
            version="v1",
            kube_client=mock_client,
            group="yelp.com",
        )
        == expected
    )


def test_create_stateful_set():
    mock_client = mock.Mock()
    mock_namespace = "paasta"
    create_stateful_set(mock_client, V1StatefulSet(api_version="some"), mock_namespace)
    mock_client.deployments.create_namespaced_stateful_set.assert_called_with(
        namespace="paasta", body=V1StatefulSet(api_version="some")
    )


def test_update_stateful_set():
    mock_client = mock.Mock()
    mock_namespace = "paasta"
    update_stateful_set(
        mock_client,
        V1StatefulSet(metadata=V1ObjectMeta(name="kurupt")),
        mock_namespace,
    )
    mock_client.deployments.replace_namespaced_stateful_set.assert_called_with(
        namespace="paasta",
        name="kurupt",
        body=V1StatefulSet(metadata=V1ObjectMeta(name="kurupt")),
    )

    mock_client = mock.Mock()
    create_stateful_set(mock_client, V1StatefulSet(api_version="some"), mock_namespace)
    mock_client.deployments.create_namespaced_stateful_set.assert_called_with(
        namespace="paasta", body=V1StatefulSet(api_version="some")
    )


def test_get_kubernetes_app_deploy_status():
    mock_status = mock.Mock(replicas=1, ready_replicas=1, updated_replicas=1)
    mock_app = mock.Mock(status=mock_status)
    assert get_kubernetes_app_deploy_status(mock_app, desired_instances=1) == (
        KubernetesDeployStatus.Running,
        "",
    )

    assert get_kubernetes_app_deploy_status(mock_app, desired_instances=2) == (
        KubernetesDeployStatus.Waiting,
        "",
    )

    mock_status = mock.Mock(replicas=1, ready_replicas=2, updated_replicas=1)
    mock_app = mock.Mock(status=mock_status)
    assert get_kubernetes_app_deploy_status(mock_app, desired_instances=2) == (
        KubernetesDeployStatus.Deploying,
        "",
    )

    mock_status = mock.Mock(replicas=0, ready_replicas=None, updated_replicas=0)
    mock_app = mock.Mock(status=mock_status)
    assert get_kubernetes_app_deploy_status(mock_app, desired_instances=0) == (
        KubernetesDeployStatus.Stopped,
        "",
    )

    mock_status = mock.Mock(replicas=0, ready_replicas=0, updated_replicas=0)
    mock_app = mock.Mock(status=mock_status)
    assert get_kubernetes_app_deploy_status(mock_app, desired_instances=0) == (
        KubernetesDeployStatus.Stopped,
        "",
    )

    mock_status = mock.Mock(replicas=1, ready_replicas=None, updated_replicas=None)
    mock_app = mock.Mock(status=mock_status)
    assert get_kubernetes_app_deploy_status(mock_app, desired_instances=1) == (
        KubernetesDeployStatus.Waiting,
        "",
    )


def test_parse_container_resources():
    partial_cpus = {"cpu": "1200m", "memory": "100Mi", "ephemeral-storage": "1Gi"}
    assert kubernetes_tools.parse_container_resources(
        partial_cpus
    ) == KubeContainerResources(1.2, 100, 1000)

    whole_cpus = {"cpu": "2", "memory": "100Mi", "ephemeral-storage": "1Gi"}
    assert kubernetes_tools.parse_container_resources(
        whole_cpus
    ) == KubeContainerResources(2, 100, 1000)

    missing_resource = {"cpu": "2", "memory": "100Mi"}
    assert kubernetes_tools.parse_container_resources(
        missing_resource
    ) == KubeContainerResources(2, 100, None)


@mock.patch.object(kubernetes_tools, "sanitise_kubernetes_name", autospec=True)
def test_get_kubernetes_app_name(mock_sanitise):
    mock_sanitise.side_effect = ["sanitised--service", "sanitised--instance"]
    app_name = kubernetes_tools.get_kubernetes_app_name("a_service", "an_instance")
    assert app_name == "sanitised--service-sanitised--instance"
    assert mock_sanitise.call_args_list == [
        mock.call("a_service"),
        mock.call("an_instance"),
    ]


def test_get_kubernetes_app_by_name():
    mock_client = mock.Mock()
    mock_deployment = mock.Mock()
    mock_client.deployments.read_namespaced_deployment_status.return_value = (
        mock_deployment
    )
    assert (
        get_kubernetes_app_by_name("someservice", mock_client, namespace="paasta")
        == mock_deployment
    )
    assert mock_client.deployments.read_namespaced_deployment_status.called
    assert not mock_client.deployments.read_namespaced_stateful_set_status.called

    mock_stateful_set = mock.Mock()
    mock_client.deployments.read_namespaced_deployment_status.reset_mock()
    mock_client.deployments.read_namespaced_deployment_status.side_effect = (
        ApiException(404)
    )
    mock_client.deployments.read_namespaced_stateful_set_status.return_value = (
        mock_stateful_set
    )
    assert (
        get_kubernetes_app_by_name("someservice", mock_client, namespace="paasta")
        == mock_stateful_set
    )
    assert mock_client.deployments.read_namespaced_deployment_status.called
    assert mock_client.deployments.read_namespaced_stateful_set_status.called


@pytest.mark.asyncio
async def test_pods_for_service_instance():
    mock_client = mock.Mock()
    assert (
        await pods_for_service_instance("kurupt", "fm", mock_client, namespace="paasta")
        == mock_client.core.list_namespaced_pod.return_value.items
    )


def test_get_active_versions_for_service():
    mock_pod_list = [
        mock.Mock(
            metadata=mock.Mock(
                labels={
                    "yelp.com/paasta_config_sha": "a123",
                    "yelp.com/paasta_git_sha": "b456",
                    "paasta.yelp.com/config_sha": "a123",
                    "paasta.yelp.com/git_sha": "b456",
                }
            )
        ),
        mock.Mock(
            metadata=mock.Mock(
                labels={
                    "yelp.com/paasta_config_sha": "a123!!!",
                    "yelp.com/paasta_git_sha": "b456!!!",
                    "paasta.yelp.com/config_sha": "a123!!!",
                    "paasta.yelp.com/git_sha": "b456!!!",
                }
            )
        ),
        mock.Mock(
            metadata=mock.Mock(
                labels={
                    "yelp.com/paasta_config_sha": "a123!!!",
                    "yelp.com/paasta_git_sha": "b456!!!",
                    "paasta.yelp.com/config_sha": "a123!!!",
                    "paasta.yelp.com/git_sha": "b456!!!",
                }
            )
        ),
        mock.Mock(
            metadata=mock.Mock(
                labels={
                    "yelp.com/paasta_config_sha": "c123",
                    "yelp.com/paasta_git_sha": "d456",
                    "paasta.yelp.com/config_sha": "c123",
                    "paasta.yelp.com/git_sha": "d456",
                    "paasta.yelp.com/image_version": "extrastuff",
                }
            )
        ),
    ]
    assert get_active_versions_for_service(mock_pod_list) == {
        (DeploymentVersion("b456!!!", None), "a123!!!"),
        (DeploymentVersion("b456", None), "a123"),
        (DeploymentVersion("d456", "extrastuff"), "c123"),
    }


def test_get_all_pods():
    mock_client = mock.Mock()
    assert (
        get_all_pods(mock_client, namespace="paasta")
        == mock_client.core.list_namespaced_pod.return_value.items
    )


def test_get_all_nodes():
    mock_client = mock.Mock()
    assert get_all_nodes(mock_client) == mock_client.core.list_node.return_value.items


def test_filter_pods_for_service_instance():
    mock_pod_1 = mock.MagicMock(
        metadata=mock.MagicMock(
            labels={
                "yelp.com/paasta_service": "kurupt",
                "yelp.com/paasta_instance": "fm",
                "paasta.yelp.com/service": "kurupt",
                "paasta.yelp.com/instance": "fm",
            }
        )
    )
    mock_pod_2 = mock.MagicMock(
        metadata=mock.MagicMock(
            labels={
                "yelp.com/paasta_service": "kurupt",
                "yelp.com/paasta_instance": "garage",
                "paasta.yelp.com/service": "kurupt",
                "paasta.yelp.com/instance": "garage",
            }
        )
    )
    mock_pod_3 = mock.MagicMock(metadata=mock.MagicMock(labels=None))
    mock_pod_4 = mock.MagicMock(metadata=mock.MagicMock(labels={"some": "thing"}))
    mock_pods = [mock_pod_1, mock_pod_2, mock_pod_3, mock_pod_4]
    assert filter_pods_by_service_instance(mock_pods, "kurupt", "fm") == [mock_pod_1]
    assert filter_pods_by_service_instance(mock_pods, "kurupt", "garage") == [
        mock_pod_2
    ]
    assert filter_pods_by_service_instance(mock_pods, "kurupt", "non-existing") == []


def test_is_pod_ready():
    mock_pod = mock.MagicMock(
        status=mock.MagicMock(
            conditions=[
                mock.MagicMock(type="Ready", status="True"),
                mock.MagicMock(type="Another", status="False"),
            ]
        )
    )
    assert is_pod_ready(mock_pod)

    mock_pod = mock.MagicMock(
        status=mock.MagicMock(
            conditions=[
                mock.MagicMock(type="Ready", status="False"),
                mock.MagicMock(type="Another", status="False"),
            ]
        )
    )
    assert not is_pod_ready(mock_pod)

    mock_pod = mock.MagicMock(
        status=mock.MagicMock(
            conditions=[mock.MagicMock(type="Another", status="False")]
        )
    )
    assert not is_pod_ready(mock_pod)

    mock_pod = mock.MagicMock(status=mock.MagicMock(conditions=None))
    assert not is_pod_ready(mock_pod)


def test_is_node_ready():
    mock_node = mock.MagicMock(
        status=mock.MagicMock(
            conditions=[
                mock.MagicMock(type="Ready", status="True"),
                mock.MagicMock(type="Another", status="False"),
            ]
        )
    )
    assert is_node_ready(mock_node)

    mock_node = mock.MagicMock(
        status=mock.MagicMock(
            conditions=[
                mock.MagicMock(type="Ready", status="False"),
                mock.MagicMock(type="Another", status="False"),
            ]
        )
    )
    assert not is_node_ready(mock_node)

    mock_node = mock.MagicMock(
        status=mock.MagicMock(
            conditions=[mock.MagicMock(type="Another", status="False")]
        )
    )
    assert not is_node_ready(mock_node)


def test_filter_nodes_by_blacklist():
    with mock.patch(
        "paasta_tools.kubernetes_tools.host_passes_whitelist", autospec=True
    ) as mock_host_passes_whitelist, mock.patch(
        "paasta_tools.kubernetes_tools.host_passes_blacklist", autospec=True
    ) as mock_host_passes_blacklist, mock.patch(
        "paasta_tools.kubernetes_tools.paasta_prefixed",
        autospec=True,
        side_effect=lambda x: x,
    ):
        mock_nodes = [mock.Mock(), mock.Mock()]
        mock_host_passes_blacklist.return_value = True
        mock_host_passes_whitelist.return_value = True
        ret = filter_nodes_by_blacklist(
            mock_nodes,
            blacklist=[("location", "westeros")],
            whitelist=("nodes", ["1", "2"]),
        )
        assert ret == mock_nodes

        mock_nodes = [mock.Mock(), mock.Mock()]
        mock_host_passes_blacklist.return_value = True
        mock_host_passes_whitelist.return_value = True
        ret = filter_nodes_by_blacklist(
            mock_nodes, blacklist=[("location", "westeros")], whitelist=None
        )
        assert ret == mock_nodes

        mock_host_passes_blacklist.return_value = True
        mock_host_passes_whitelist.return_value = False
        ret = filter_nodes_by_blacklist(
            mock_nodes,
            blacklist=[("location", "westeros")],
            whitelist=("nodes", ["1", "2"]),
        )
        assert ret == []

        mock_host_passes_blacklist.return_value = False
        mock_host_passes_whitelist.return_value = True
        ret = filter_nodes_by_blacklist(
            mock_nodes,
            blacklist=[("location", "westeros")],
            whitelist=("nodes", ["1", "2"]),
        )
        assert ret == []

        mock_host_passes_blacklist.return_value = False
        mock_host_passes_whitelist.return_value = False
        ret = filter_nodes_by_blacklist(
            mock_nodes,
            blacklist=[("location", "westeros")],
            whitelist=("nodes", ["1", "2"]),
        )
        assert ret == []


def test_get_nodes_grouped_by_attribute():
    with mock.patch(
        "paasta_tools.kubernetes_tools.paasta_prefixed",
        autospec=True,
        side_effect=lambda x: x,
    ):
        mock_node_1 = mock.MagicMock(
            metadata=mock.MagicMock(labels={"region": "westeros"})
        )
        mock_node_2 = mock.MagicMock(
            metadata=mock.MagicMock(labels={"region": "middle-earth"})
        )
        assert get_nodes_grouped_by_attribute([mock_node_1, mock_node_2], "region") == {
            "westeros": [mock_node_1],
            "middle-earth": [mock_node_2],
        }
        assert (
            get_nodes_grouped_by_attribute([mock_node_1, mock_node_2], "superregion")
            == {}
        )


def test_paasta_prefixed():
    assert paasta_prefixed("kubernetes.io/thing") == "kubernetes.io/thing"
    assert paasta_prefixed("region") == "yelp.com/region"


def test_sanitise_kubernetes_name():
    assert sanitise_kubernetes_name("my_service") == "my--service"
    assert sanitise_kubernetes_name("MY_SERVICE") == "my--service"
    assert sanitise_kubernetes_name("myservice") == "myservice"
    assert sanitise_kubernetes_name("_shared") == "underscore-shared"
    assert sanitise_kubernetes_name("_shared_thing") == "underscore-shared--thing"


@pytest.mark.parametrize(
    "namespace, secret, secret_data",
    [
        ("paasta", "mortys-fate", "ab1234"),
        ("tron", "mortys-fate", "ab1234"),
    ],
)
def test_create_kubernetes_secret_signature(namespace, secret, secret_data):
    mock_client = mock.Mock()
    create_secret_signature(
        kube_client=mock_client,
        signature_name=get_paasta_secret_signature_name(namespace, "universe", secret),
        service_name="universe",
        secret_signature="ab1234",
        namespace=namespace,
    )
    assert mock_client.core.create_namespaced_config_map.called
    _, kwargs = mock_client.core.create_namespaced_config_map.call_args
    assert kwargs.get("namespace") == namespace
    assert (
        kwargs.get("body").metadata.name
        == f"{namespace}-secret-universe-{sanitise_kubernetes_name(secret)}-signature"
    )


@pytest.mark.parametrize(
    "namespace, secret, secret_signature",
    [
        ("paasta", "mortys-fate", "ab1234"),
        ("tron", "mortys-fate", "abc4321"),
    ],
)
def test_update_kubernetes_secret_signature(namespace, secret, secret_signature):
    mock_client = mock.Mock()
    update_secret_signature(
        kube_client=mock_client,
        service_name="universe",
        signature_name=get_paasta_secret_signature_name(
            namespace, "universe", secret_signature
        ),
        secret_signature=secret_signature,
        namespace=namespace,
    )
    assert mock_client.core.replace_namespaced_config_map.called
    _, kwargs = mock_client.core.replace_namespaced_config_map.call_args
    assert kwargs.get("namespace") == namespace


@pytest.mark.parametrize(
    "namespace, secret, secret_signature",
    [
        ("paasta", "mortys-morty", "hancock"),
        ("tron", "mortys-morty", "hungry"),
    ],
)
def test_get_kubernetes_secret_signature(namespace, secret, secret_signature):
    mock_client = mock.Mock()
    mock_client.core.read_namespaced_config_map.return_value = mock.Mock(
        data={"signature": secret_signature}
    )

    secret_sig = get_secret_signature(
        kube_client=mock_client,
        signature_name=get_paasta_secret_signature_name(namespace, "universe", secret),
        namespace=namespace,
    )
    _, kwargs = mock_client.core.read_namespaced_config_map.call_args
    assert kwargs.get("namespace") == namespace
    assert secret_sig == secret_signature
    assert (
        kwargs["name"]
        == f"{namespace}-secret-universe-{sanitise_kubernetes_name(secret)}-signature"
    )


def test_get_kubernetes_secret_signature_404():
    mock_client = mock.Mock()
    mock_client.core.read_namespaced_config_map.side_effect = ApiException(404)
    assert (
        get_secret_signature(
            kube_client=mock_client,
            signature_name="paasta-secret-foo-signature",
            namespace="paasta",
        )
        is None
    )


def test_get_kubernetes_secret_signature_401():
    mock_client = mock.Mock()
    mock_client.core.read_namespaced_config_map.side_effect = ApiException(401)
    with pytest.raises(ApiException):
        get_secret_signature(
            kube_client=mock_client,
            signature_name="paasta-secret-foo-signature",
            namespace="paasta",
        )


@pytest.mark.parametrize(
    "namespace, secret, secret_data",
    [
        ("paasta", "mortys-fate", {"mortys-fate": "Zm9v"}),
        ("tron", "mortys_fate", {"foo": "boo"}),
    ],
)
def test_create_secret(namespace, secret, secret_data):
    mock_client = mock.Mock()
    create_secret(
        kube_client=mock_client,
        service_name="universe",
        secret_name=get_paasta_secret_name(namespace, "universe", secret),
        secret_data=secret_data,
        namespace=namespace,
    )
    assert mock_client.core.create_namespaced_secret.called
    _, kwargs = mock_client.core.create_namespaced_secret.call_args
    assert kwargs.get("namespace") == namespace
    assert kwargs["body"].data == secret_data
    assert (
        kwargs["body"].metadata.name
        == f"{namespace}-secret-universe-{sanitise_kubernetes_name(secret)}"
    )


@pytest.mark.parametrize(
    "namespace,secret,secret_data",
    [
        ("paasta", "mortys-fate", {"fury": "bury"}),
        ("tron", "mortys_fate", {"ant": "pant"}),
    ],
)
def test_update_secret(namespace, secret, secret_data):
    mock_client = mock.Mock()

    update_secret(
        kube_client=mock_client,
        service_name="universe",
        secret_name=get_paasta_secret_name(namespace, "universe", secret),
        secret_data=secret_data,
        namespace=namespace,
    )
    assert mock_client.core.replace_namespaced_secret.called
    _, kwargs = mock_client.core.replace_namespaced_secret.call_args
    assert kwargs.get("namespace") == namespace
    assert kwargs["body"].data == secret_data
    assert (
        kwargs["body"].metadata.name
        == f"{namespace}-secret-universe-{sanitise_kubernetes_name(secret)}"
    )


def test_get_kubernetes_secret_hashes():
    with mock.patch(
        "paasta_tools.kubernetes_tools.KubeClient", autospec=True
    ) as mock_client, mock.patch(
        "paasta_tools.kubernetes_tools.is_secret_ref", autospec=True
    ) as mock_is_secret_ref, mock.patch(
        "paasta_tools.kubernetes_tools.get_secret_signature",
        autospec=True,
        return_value="somesig",
    ) as mock_get_kubernetes_secret_signature, mock.patch(
        "paasta_tools.kubernetes_tools.is_shared_secret", autospec=True
    ) as mock_is_shared_secret:
        mock_is_secret_ref.side_effect = lambda x: False if x == "ASECRET" else True
        mock_is_shared_secret.side_effect = (
            lambda x: False if not x.startswith("SHARED") else True
        )

        hashes = get_kubernetes_secret_hashes(
            environment_variables={
                "A": "SECRET(ref)",
                "NOT": "ASECRET",
                "SOME": "SHAREDSECRET(ref1)",
            },
            service="universe",
            namespace="paasta",
        )
        mock_get_kubernetes_secret_signature.assert_has_calls(
            [
                mock.call(
                    kube_client=mock_client.return_value,
                    signature_name=get_paasta_secret_signature_name(
                        "paasta",
                        "universe",
                        get_secret_name_from_ref("SECRET(ref)"),
                    ),
                    namespace="paasta",
                ),
                mock.call(
                    kube_client=mock_client.return_value,
                    signature_name=get_paasta_secret_signature_name(
                        "paasta",
                        SHARED_SECRET_SERVICE,
                        get_secret_name_from_ref("SECRET(ref1)"),
                    ),
                    namespace="paasta",
                ),
            ]
        )
        assert hashes == {"SECRET(ref)": "somesig", "SHAREDSECRET(ref1)": "somesig"}


def test_load_custom_resources():
    mock_resources = [
        {
            "version": "v1",
            "kube_kind": {"plural": "Flinks", "singular": "flink"},
            "file_prefix": "flink",
            "group": "yelp.com",
        }
    ]
    mock_config = mock.Mock(
        get_kubernetes_custom_resources=mock.Mock(return_value=mock_resources)
    )
    assert kubernetes_tools.load_custom_resource_definitions(mock_config) == [
        kubernetes_tools.CustomResourceDefinition(
            version="v1",
            kube_kind=kubernetes_tools.KubeKind(plural="Flinks", singular="flink"),
            file_prefix="flink",
            group="yelp.com",
        )
    ]


def test_warning_big_bounce_default_config():
    job_config = kubernetes_tools.KubernetesDeploymentConfig(
        service="service",
        instance="instance",
        cluster="cluster",
        config_dict={},
        branch_dict={
            "docker_image": "abcdef",
            "git_sha": "deadbeef",
            "image_version": None,
            "force_bounce": None,
            "desired_state": "start",
        },
    )

    with mock.patch(
        "paasta_tools.utils.load_system_paasta_config",
        return_value=SystemPaastaConfig(
            {
                "volumes": [],
                "hacheck_sidecar_volumes": [],
                "expected_slave_attributes": [{"region": "blah"}],
                "docker_registry": "docker-registry.local",
            },
            "/fake/dir/",
        ),
        autospec=True,
    ) as mock_load_system_paasta_config, mock.patch(
        "paasta_tools.kubernetes_tools.load_system_paasta_config",
        new=mock_load_system_paasta_config,
        autospec=False,
    ), mock.patch(
        "paasta_tools.kubernetes_tools.load_service_namespace_config",
        return_value=ServiceNamespaceConfig(),
        autospec=True,
    ):
        assert (
            job_config.format_kubernetes_app().spec.template.metadata.labels[
                "paasta.yelp.com/config_sha"
            ]
            == "config84789e0b"
        ), "If this fails, just change the constant in this test, but be aware that deploying this change will cause every service to bounce!"


def test_warning_big_bounce_routable_pod():
    job_config = kubernetes_tools.KubernetesDeploymentConfig(
        service="service",
        instance="instance",
        cluster="cluster",
        config_dict={
            "registrations": ["service.instance"],
        },
        branch_dict={
            "docker_image": "abcdef",
            "git_sha": "deadbeef",
            "image_version": None,
            "force_bounce": None,
            "desired_state": "start",
        },
    )

    with mock.patch(
        "paasta_tools.utils.load_system_paasta_config",
        return_value=SystemPaastaConfig(
            {
                "volumes": [],
                "hacheck_sidecar_volumes": [],
                "expected_slave_attributes": [{"region": "blah"}],
                "docker_registry": "docker-registry.local",
            },
            "/fake/dir/",
        ),
        autospec=True,
    ) as mock_load_system_paasta_config, mock.patch(
        "paasta_tools.kubernetes_tools.load_system_paasta_config",
        new=mock_load_system_paasta_config,
        autospec=False,
    ), mock.patch(
        "paasta_tools.kubernetes_tools.load_service_namespace_config",
        return_value=ServiceNamespaceConfig({"proxy_port": 1}),
        autospec=True,
    ):
        assert (
            job_config.format_kubernetes_app().spec.template.metadata.labels[
                "paasta.yelp.com/config_sha"
            ]
            == "config46a479f2"
        ), "If this fails, just change the constant in this test, but be aware that deploying this change will cause every smartstack-registered service to bounce!"


def test_warning_big_bounce_common_config():
    job_config = kubernetes_tools.KubernetesDeploymentConfig(
        service="service",
        instance="instance",
        cluster="cluster",
        config_dict={
            # XXX: this should include other common options that are used
            "cap_add": ["SET_GID"],
        },
        branch_dict={
            "docker_image": "abcdef",
            "git_sha": "deadbeef",
            "image_version": None,
            "force_bounce": None,
            "desired_state": "start",
        },
    )

    with mock.patch(
        "paasta_tools.utils.load_system_paasta_config",
        return_value=SystemPaastaConfig(
            {
                "volumes": [],
                "hacheck_sidecar_volumes": [],
                "expected_slave_attributes": [{"region": "blah"}],
                "docker_registry": "docker-registry.local",
            },
            "/fake/dir/",
        ),
        autospec=True,
    ) as mock_load_system_paasta_config, mock.patch(
        "paasta_tools.kubernetes_tools.load_system_paasta_config",
        new=mock_load_system_paasta_config,
        autospec=False,
    ), mock.patch(
        "paasta_tools.kubernetes_tools.load_service_namespace_config",
        return_value=ServiceNamespaceConfig(),
        autospec=True,
    ):
        assert (
            job_config.format_kubernetes_app().spec.template.metadata.labels[
                "paasta.yelp.com/config_sha"
            ]
            == "confige61d940f"
        ), "If this fails, just change the constant in this test, but be aware that deploying this change will cause every service to bounce!"


@pytest.mark.parametrize(
    "pod_node_name,node,expected",
    [
        (  # ok case
            "a_node_name",
            mock.Mock(metadata=mock.Mock(labels={"yelp.com/hostname": "a_hostname"})),
            "a_hostname",
        ),
        # error case: no node name, not scheduled
        (None, ApiException(), "NotScheduled"),
        # pod has no hostname label, default to node_name
        (
            "a_node_name",
            mock.Mock(metadata=mock.Mock(labels={})),
            "a_node_name",
        ),
        # ApiException, default to node_name
        (
            "a_node_name",
            ApiException(),
            "a_node_name",
        ),
    ],
)
def test_get_pod_hostname(pod_node_name, node, expected):
    client = mock.MagicMock()
    client.core.read_node.side_effect = [node]
    pod = mock.MagicMock()
    pod.spec.node_name = pod_node_name

    hostname = kubernetes_tools.get_pod_hostname(client, pod)

    assert hostname == expected


def test_get_pod_node():
    with mock.patch(
        "paasta_tools.kubernetes_tools.get_all_nodes",
        autospec=True,
    ) as mock_get_all_nodes:
        mock_name_1 = mock.Mock()
        mock_name_2 = mock.Mock()
        type(mock_name_1).name = "node1"
        mock_node_1 = mock.Mock(metadata=mock_name_1)
        type(mock_name_2).name = "node2"
        mock_node_2 = mock.Mock(metadata=mock_name_2)
        mock_get_all_nodes.return_value = [mock_node_1, mock_node_2]
        mock_pod = mock.Mock()
        type(mock_pod).spec = mock.Mock(node_name="node1")
        assert kubernetes_tools.get_pod_node(mock.Mock(), mock_pod) == mock_node_1

        mock_get_all_nodes.return_value = [mock_node_1, mock_node_2]
        type(mock_pod).spec = mock.Mock(node_name="node3")
        assert kubernetes_tools.get_pod_node(mock.Mock(), mock_pod) is None


@pytest.mark.parametrize(
    "label,expected",
    [
        ("a_random_label", "a_random_label"),  # non-special case
        ("instance_type", "node.kubernetes.io/instance-type"),  # instance_type
        ("habitat", "yelp.com/habitat"),  # hiera case
    ],
)
def test_to_node_label(label, expected):
    assert kubernetes_tools.to_node_label(label) == expected


def test_mode_to_int():
    assert mode_to_int(None) is None
    assert mode_to_int(0o123) == 0o123
    assert mode_to_int("0123") == 0o123


def test_running_task_allocation_get_kubernetes_metadata():
    mock_pod = mock.MagicMock()
    mock_pod.metadata.labels = {
        "yelp.com/paasta_service": "srv1",
        "yelp.com/paasta_instance": "instance1",
        "paasta.yelp.com/service": "srv1",
        "paasta.yelp.com/instance": "instance1",
        "paasta.yelp.com/git_sha": "30cc51cff849871c7de3cc39ed951a7914894dac",
        "paasta.yelp.com/config_sha": "config399d26e6",
    }
    mock_pod.metadata.name = "pod_1"
    mock_pod.status.pod_ip = "10.10.10.10"
    mock_pod.status.host_ip = "10.10.10.11"
    ret = task_allocation_get_kubernetes_metadata(mock_pod)
    assert ret == (
        "srv1",
        "instance1",
        "pod_1",
        "10.10.10.10",
        "10.10.10.11",
        "30cc51cff849871c7de3cc39ed951a7914894dac",
        "config399d26e6",
    )


def test_running_task_allocation_get_pod_pool():
    mock_node = mock.MagicMock()
    mock_node.metadata.labels = {"paasta.yelp.com/pool": "foo"}
    with mock.patch(
        "paasta_tools.kubernetes_tools.get_pod_node",
        autospec=True,
        return_value=mock_node,
    ):
        ret = task_allocation_get_pod_pool(mock.Mock(), mock.Mock())
        assert ret == "foo"
        mock_node.metadata.labels = None

        ret = task_allocation_get_pod_pool(mock.Mock(), mock.Mock())
        assert ret == "default"


@pytest.mark.parametrize(
    "config_dict, expected_management_policy",
    [
        ({"pod_management_policy": "Parallel"}, "Parallel"),
        ({}, "OrderedReady"),
    ],
)
def test_get_pod_management_policy(config_dict, expected_management_policy):
    deployment = KubernetesDeploymentConfig(
        service="my-service",
        instance="my-instance",
        cluster="mega-cluster",
        config_dict=config_dict,
        branch_dict=None,
        soa_dir="/nail/blah",
    )
    assert deployment.get_pod_management_policy() == expected_management_policy


def test_ensure_service_account_new():
    iam_role = "arn:aws:iam::000000000000:role/some_role"
    namespace = "test_namespace"
    k8s_role = None
    expected_sa_name = "paasta--arn-aws-iam-000000000000-role-some-role"
    with mock.patch(
        "paasta_tools.kubernetes_tools.kube_config.load_kube_config", autospec=True
    ), mock.patch(
        "paasta_tools.kubernetes_tools.KubeClient",
        autospec=False,
    ) as mock_kube_client:
        mock_client = mock.Mock()
        mock_client.core = mock.Mock(spec=kube_client.CoreV1Api)
        mock_client.rbac = mock.Mock(spec=kube_client.RbacAuthorizationV1Api)
        mock_client.core.list_namespaced_service_account.return_value = mock.Mock(
            spec=V1ServiceAccountList
        )
        mock_client.core.list_namespaced_service_account.return_value.items = []
        mock_kube_client.return_value = mock_client

        ensure_service_account(
            iam_role, namespace=namespace, kube_client=mock_client, k8s_role=k8s_role
        )
        mock_client.core.create_namespaced_service_account.assert_called_once_with(
            namespace=namespace,
            body=V1ServiceAccount(
                kind="ServiceAccount",
                metadata=V1ObjectMeta(
                    name=expected_sa_name,
                    namespace=namespace,
                    annotations={"eks.amazonaws.com/role-arn": iam_role},
                ),
            ),
        )
        mock_client.rbac.create_namespaced_role_binding.assert_not_called()


def test_ensure_service_account_with_k8s_role_new():
    iam_role = "arn:aws:iam::000000000000:role/some_role"
    namespace = "test_namespace"
    k8s_role = "mega-admin"
    expected_sa_name = "paasta--arn-aws-iam-000000000000-role-some-role--mega-admin"
    with mock.patch(
        "paasta_tools.kubernetes_tools.kube_config.load_kube_config", autospec=True
    ), mock.patch(
        "paasta_tools.kubernetes_tools.KubeClient",
        autospec=False,
    ) as mock_kube_client:
        mock_client = mock.Mock()
        mock_client.core = mock.Mock(spec=kube_client.CoreV1Api)
        mock_client.rbac = mock.Mock(spec=kube_client.RbacAuthorizationV1Api)
        mock_client.core.list_namespaced_service_account.return_value = mock.Mock(
            spec=V1ServiceAccountList
        )
        mock_client.core.list_namespaced_service_account.return_value.items = []
        mock_client.rbac.list_namespaced_role_binding.return_value = mock.Mock(
            spec=V1RoleBinding,
        )
        mock_client.rbac.list_namespaced_role_binding.return_value.items = []
        mock_kube_client.return_value = mock_client

        ensure_service_account(
            iam_role, namespace=namespace, kube_client=mock_client, k8s_role=k8s_role
        )
        mock_client.core.create_namespaced_service_account.assert_called_once_with(
            namespace=namespace,
            body=V1ServiceAccount(
                kind="ServiceAccount",
                metadata=V1ObjectMeta(
                    name=expected_sa_name,
                    namespace=namespace,
                    annotations={"eks.amazonaws.com/role-arn": iam_role},
                ),
            ),
        )
        mock_client.rbac.create_namespaced_role_binding.assert_called_once_with(
            namespace=namespace,
            body=V1RoleBinding(
                metadata=V1ObjectMeta(
                    name=expected_sa_name,
                    namespace=namespace,
                ),
                role_ref=V1RoleRef(
                    api_group="rbac.authorization.k8s.io",
                    kind="Role",
                    name=k8s_role,
                ),
                subjects=[
                    V1Subject(
                        kind="ServiceAccount",
                        namespace=namespace,
                        name=expected_sa_name,
                    ),
                ],
            ),
        )


def test_ensure_service_account_existing():
    iam_role = "arn:aws:iam::000000000000:role/some_role"
    namespace = "test_namespace"
    k8s_role = "mega-admin"
    expected_sa_name = "paasta--arn-aws-iam-000000000000-role-some-role--mega-admin"
    with mock.patch(
        "paasta_tools.kubernetes_tools.kube_config.load_kube_config", autospec=True
    ), mock.patch(
        "paasta_tools.kubernetes_tools.KubeClient",
        autospec=False,
    ) as mock_kube_client:
        mock_client = mock.Mock()
        mock_client.core = mock.Mock(spec=kube_client.CoreV1Api)
        mock_client.rbac = mock.Mock(spec=kube_client.RbacAuthorizationV1Api)
        mock_client.core.list_namespaced_service_account.return_value = mock.Mock(
            spec=V1ServiceAccountList
        )
        mock_client.core.list_namespaced_service_account.return_value.items = [
            V1ServiceAccount(
                kind="ServiceAccount",
                metadata=V1ObjectMeta(
                    name=expected_sa_name,
                    namespace=namespace,
                    annotations={"eks.amazonaws.com/role-arn": iam_role},
                ),
            )
        ]
        mock_client.rbac.list_namespaced_role_binding.return_value = mock.Mock(
            spec=V1RoleBinding,
        )
        mock_client.rbac.list_namespaced_role_binding.return_value.items = [
            V1RoleBinding(
                kind="ServiceAccount",
                metadata=V1ObjectMeta(
                    name=expected_sa_name,
                    namespace=namespace,
                ),
                role_ref=V1RoleRef(
                    api_group="rbac.authorization.k8s.io",
                    kind="Role",
                    name=k8s_role,
                ),
                subjects=[
                    V1Subject(
                        kind="ServiceAccount",
                        namespace=namespace,
                        name=expected_sa_name,
                    )
                ],
            )
        ]
        mock_kube_client.return_value = mock_client

        ensure_service_account(
            iam_role, namespace=namespace, kube_client=mock_client, k8s_role=k8s_role
        )
        mock_client.core.create_namespaced_service_account.assert_not_called()
        mock_client.rbac.create_namespaced_role_binding.assert_not_called()


def test_ensure_service_account_existing_different_role():
    old_iam_role = "arn:aws:iam::000000000000:role/some_role"
    new_iam_role = "arn:aws:iam::000000000000:role/some-role"
    namespace = "test_namespace"
    k8s_role = "mega-admin"
    expected_sa_name = "paasta--arn-aws-iam-000000000000-role-some-role--mega-admin"
    with mock.patch(
        "paasta_tools.kubernetes_tools.kube_config.load_kube_config", autospec=True
    ), mock.patch(
        "paasta_tools.kubernetes_tools.KubeClient",
        autospec=False,
    ) as mock_kube_client:
        mock_client = mock.Mock()
        mock_client.core = mock.Mock(spec=kube_client.CoreV1Api)
        mock_client.rbac = mock.Mock(spec=kube_client.RbacAuthorizationV1Api)
        mock_client.core.list_namespaced_service_account.return_value = mock.Mock(
            spec=V1ServiceAccountList
        )
        mock_client.core.list_namespaced_service_account.return_value.items = [
            V1ServiceAccount(
                kind="ServiceAccount",
                metadata=V1ObjectMeta(
                    name=expected_sa_name,
                    namespace=namespace,
                    annotations={"eks.amazonaws.com/role-arn": old_iam_role},
                ),
            )
        ]
        mock_client.rbac.list_namespaced_role_binding.return_value = mock.Mock(
            spec=V1RoleBinding,
        )
        mock_client.rbac.list_namespaced_role_binding.return_value.items = [
            V1RoleBinding(
                kind="ServiceAccount",
                metadata=V1ObjectMeta(
                    name=expected_sa_name,
                    namespace=namespace,
                ),
                role_ref=V1RoleRef(
                    api_group="rbac.authorization.k8s.io",
                    kind="Role",
                    name=k8s_role,
                ),
                subjects=[
                    V1Subject(
                        kind="ServiceAccount",
                        namespace=namespace,
                        name=expected_sa_name,
                    )
                ],
            )
        ]
        mock_kube_client.return_value = mock_client
        ensure_service_account(
            new_iam_role,
            namespace=namespace,
            kube_client=mock_client,
            k8s_role=k8s_role,
        )
        mock_client.core.create_namespaced_service_account.assert_not_called()
        mock_client.core.patch_namespaced_service_account.assert_called_once_with(
            namespace=namespace,
            body=V1ServiceAccount(
                kind="ServiceAccount",
                metadata=V1ObjectMeta(
                    name=expected_sa_name,
                    namespace=namespace,
                    annotations={"eks.amazonaws.com/role-arn": new_iam_role},
                ),
            ),
            name=expected_sa_name,
        )


def test_ensure_service_account_existing_create_rb_only():
    iam_role = "arn:aws:iam::000000000000:role/some_role"
    namespace = "test_namespace"
    k8s_role = "mega-admin"
    expected_sa_name = "paasta--arn-aws-iam-000000000000-role-some-role--mega-admin"
    with mock.patch(
        "paasta_tools.kubernetes_tools.kube_config.load_kube_config", autospec=True
    ), mock.patch(
        "paasta_tools.kubernetes_tools.KubeClient",
        autospec=False,
    ) as mock_kube_client:
        mock_client = mock.Mock()
        mock_client.core = mock.Mock(spec=kube_client.CoreV1Api)
        mock_client.rbac = mock.Mock(spec=kube_client.RbacAuthorizationV1Api)
        mock_client.core.list_namespaced_service_account.return_value = mock.Mock(
            spec=V1ServiceAccountList
        )
        mock_client.core.list_namespaced_service_account.return_value.items = [
            V1ServiceAccount(
                kind="ServiceAccount",
                metadata=V1ObjectMeta(
                    name=expected_sa_name,
                    namespace=namespace,
                    annotations={"eks.amazonaws.com/role-arn": iam_role},
                ),
            )
        ]
        mock_client.rbac.list_namespaced_role_binding.return_value = mock.Mock(
            spec=V1RoleBinding,
        )
        mock_client.rbac.list_namespaced_role_binding.return_value.items = []
        mock_kube_client.return_value = mock_client

        ensure_service_account(
            iam_role, namespace=namespace, kube_client=mock_client, k8s_role=k8s_role
        )
        mock_client.core.create_namespaced_service_account.assert_not_called()
        assert mock_client.rbac.create_namespaced_role_binding.called is True


def test_ensure_service_account_caps():
    iam_role = "arn:aws:iam::000000000000:role/Some_Role"
    expected_sa_name = "paasta--arn-aws-iam-000000000000-role-some-role"
    with mock.patch(
        "paasta_tools.kubernetes_tools.kube_config.load_kube_config", autospec=True
    ):
        assert expected_sa_name == get_service_account_name(
            iam_role,
        )


def test_ensure_service_account_caps_with_k8s():
    iam_role = "arn:aws:iam::000000000000:role/Some_Role"
    namespace = "test_namespace"
    k8s_role = "mega-admin"
    expected_sa_name = "paasta--arn-aws-iam-000000000000-role-some-role--mega-admin"
    with mock.patch(
        "paasta_tools.kubernetes_tools.kube_config.load_kube_config", autospec=True
    ), mock.patch(
        "paasta_tools.kubernetes_tools.KubeClient",
        autospec=False,
    ) as mock_kube_client:
        mock_client = mock.Mock()
        mock_client.core = mock.Mock(spec=kube_client.CoreV1Api)
        mock_client.rbac = mock.Mock(spec=kube_client.RbacAuthorizationV1Api)
        mock_client.core.list_namespaced_service_account.return_value = mock.Mock(
            spec=V1ServiceAccountList
        )
        mock_client.core.list_namespaced_service_account.return_value.items = [
            V1ServiceAccount(
                kind="ServiceAccount",
                metadata=V1ObjectMeta(
                    name=expected_sa_name,
                    namespace=namespace,
                    annotations={"eks.amazonaws.com/role-arn": iam_role},
                ),
            )
        ]
        mock_client.rbac.list_namespaced_role_binding.return_value = mock.Mock(
            spec=V1RoleBinding,
        )
        mock_client.rbac.list_namespaced_role_binding.return_value.items = [
            V1RoleBinding(
                kind="ServiceAccount",
                metadata=V1ObjectMeta(
                    name=expected_sa_name,
                    namespace=namespace,
                ),
                role_ref=V1RoleRef(
                    api_group="rbac.authorization.k8s.io",
                    kind="Role",
                    name=k8s_role,
                ),
                subjects=[
                    V1Subject(
                        kind="ServiceAccount",
                        namespace=namespace,
                        name=expected_sa_name,
                    )
                ],
            )
        ]
        mock_kube_client.return_value = mock_client

        ensure_service_account(
            iam_role, namespace=namespace, kube_client=mock_client, k8s_role=k8s_role
        )
        mock_client.core.create_namespaced_service_account.assert_not_called()
        mock_client.rbac.create_namespaced_role_binding.assert_not_called()


@pytest.mark.parametrize("decode", [(True), (False)])
def test_get_kubernetes_secret(decode):
    with mock.patch(
        "paasta_tools.kubernetes_tools.KubeClient",
        autospec=True,
    ) as mock_kube_client, mock.patch(
        "paasta_tools.kubernetes_tools.os.environ", autospec=True
    ) as mock_env, mock.patch(
        "paasta_tools.kubernetes_tools.KubeClient", autospec=True
    ) as mock_kube_client:
        mock_namespace = "paasta"
        service_name = "example_service"
        secret_name = "example_secret"
        mock_env.return_value = {}

        mock_client = mock.Mock()
        mock_client.core = mock.Mock(spec=kube_client.CoreV1Api)
        mock_client.rbac = mock.Mock(spec=kube_client.RbacAuthorizationV1Api)
        mock_client.core.read_namespaced_secret.return_value = mock.Mock(spec=V1Secret)
        mock_client.core.read_namespaced_secret.return_value = V1Secret(
            data={"example_secret": b64encode("something".encode())},
            metadata=V1ObjectMeta(name="example_secret"),
        )
        mock_kube_client.return_value = mock_client

        ret = get_secret(
            mock_client,
            get_paasta_secret_name(mock_namespace, service_name, secret_name),
            secret_name,
            namespace=mock_namespace,
            decode=decode,
        )
        mock_client.core.read_namespaced_secret.assert_called_with(
            name="paasta-secret-example--service-example--secret", namespace="paasta"
        )
        assert ret == "something" if decode else b"something"


def test_get_kubernetes_secret_env_variables():
    with mock.patch(
        "paasta_tools.kubernetes_tools.is_secret_ref",
        autospec=True,
    ) as mock_is_secret_ref, mock.patch(
        "paasta_tools.kubernetes_tools.get_secret_name_from_ref", autospec=True
    ) as mock_get_ref, mock.patch(
        "paasta_tools.kubernetes_tools.get_secret", autospec=True
    ) as mock_get_kubernetes_secret, mock.patch(
        "paasta_tools.kubernetes_tools.KubeClient", autospec=True
    ) as mock_kube_client:
        mock_environment = {
            "MY": "aaa",
            "SECRET_NAME1": "SECRET(SECRET_NAME1)",
            "SECRET_NAME2": "SECRET(SECRET_NAME2)",
            "SHARED_SECRET1": "SHARED_SECRET(SHARED_SECRET1)",
        }

        mock_is_secret_ref.side_effect = lambda val: "SECRET" in val
        mock_get_ref.side_effect = ["SECRET_NAME1", "SECRET_NAME2", "SHARED_SECRET1"]
        mock_get_kubernetes_secret.side_effect = ["123", "abc", "shared"]
        mock_client = mock.Mock()
        mock_kube_client.return_value = mock_client

        ret = get_kubernetes_secret_env_variables(
            kube_client=mock_client,
            environment=mock_environment,
            service_name="universe",
            namespace="paasta",
        )
        assert ret == {
            "SECRET_NAME1": "123",
            "SECRET_NAME2": "abc",
            "SHARED_SECRET1": "shared",
        }

        assert mock_get_kubernetes_secret.call_args_list == [
            mock.call(
                mock_client,
                secret_name=get_paasta_secret_name(
                    "paasta", "universe", "SECRET_NAME1"
                ),
                key_name="SECRET_NAME1",
                decode=True,
                namespace="paasta",
            ),
            mock.call(
                mock_client,
                secret_name=get_paasta_secret_name(
                    "paasta", "universe", "SECRET_NAME2"
                ),
                key_name="SECRET_NAME2",
                decode=True,
                namespace="paasta",
            ),
            mock.call(
                mock_client,
                secret_name=get_paasta_secret_name(
                    "paasta", SHARED_SECRET_SERVICE, "SHARED_SECRET1"
                ),
                key_name="SHARED_SECRET1",
                decode=True,
                namespace="paasta",
            ),
        ]


def test_get_kubernetes_secret_volumes_multiple_files():
    with mock.patch(
        "paasta_tools.kubernetes_tools.get_secret", autospec=True
    ) as mock_get_kubernetes_secret, mock.patch(
        "paasta_tools.kubernetes_tools.KubeClient", autospec=True
    ) as mock_kube_client:
        mock_secret_volumes_config = [
            SecretVolume(
                container_path="/the/container/path/",
                items=[
                    {"key": "the_secret_name1", "path": "the_secret_filename1"},
                    {"key": "the_secret_name2", "path": "the_secret_filename2"},
                ],
            )
        ]

        mock_get_kubernetes_secret.side_effect = [
            "secret_contents1",
            "secret_contents2",
        ]
        mock_client = mock.Mock()
        mock_kube_client.return_value = mock_client

        ret = get_kubernetes_secret_volumes(
            kube_client=mock_client,
            secret_volumes_config=mock_secret_volumes_config,
            service_name="universe",
            namespace="paasta",
        )
        assert ret == {
            "/the/container/path/the_secret_filename1": "secret_contents1",
            "/the/container/path/the_secret_filename2": "secret_contents2",
        }


def test_get_kubernetes_secret_volumes_single_file():
    with mock.patch(
        "paasta_tools.kubernetes_tools.get_secret", autospec=True
    ) as mock_get_kubernetes_secret, mock.patch(
        "paasta_tools.kubernetes_tools.KubeClient", autospec=True
    ) as mock_kube_client:
        mock_secret_volumes_config = [
            SecretVolume(
                container_path="/the/container/path/",
                secret_name="the_secret_name",
            )
        ]

        mock_get_kubernetes_secret.side_effect = ["secret_contents"]
        mock_client = mock.Mock()
        mock_kube_client.return_value = mock_client

        ret = get_kubernetes_secret_volumes(
            kube_client=mock_client,
            secret_volumes_config=mock_secret_volumes_config,
            service_name="universe",
            namespace="paasta",
        )
        assert ret == {
            "/the/container/path/the_secret_name": "secret_contents",
        }


@pytest.mark.parametrize(
    "service,existing_config,expected",
    (
        (
            "service_auth",
            [],
            [{"audience": "foo.bar", "container_path": "/var/secret/something"}],
        ),
        ("service_noauth", [], []),
        (
            "service_auth",
            [{"audience": "foo.bar", "container_path": "/var/secret/something"}],
            [{"audience": "foo.bar", "container_path": "/var/secret/something"}],
        ),
        (
            "service_auth",
            [{"audience": "foo.bar", "container_path": "/var/secret/whatever"}],
            [
                {"audience": "foo.bar", "container_path": "/var/secret/something"},
                {"audience": "foo.bar", "container_path": "/var/secret/whatever"},
            ],
        ),
        (
            "service_noauth",
            [{"audience": "foo.bar", "container_path": "/var/secret/whatever"}],
            [{"audience": "foo.bar", "container_path": "/var/secret/whatever"}],
        ),
    ),
)
@mock.patch("paasta_tools.kubernetes_tools.load_system_paasta_config", autospec=None)
@mock.patch("paasta_tools.kubernetes_tools.get_authenticating_services", autospec=None)
def test_add_volumes_for_authenticating_services(
    mock_get_auth_services, mock_system_config, service, existing_config, expected
):
    mock_get_auth_services.return_value = {"service_auth", "service_foobar"}
    mock_system_config.return_value.get_service_auth_token_volume_config.return_value = {
        "audience": "foo.bar",
        "container_path": "/var/secret/something",
    }
    existing_config_copy = deepcopy(existing_config)
    assert (
        add_volumes_for_authenticating_services(
            service, existing_config, "/mock/soa/dir"
        )
        == expected
    )
    mock_get_auth_services.assert_called_once_with("/mock/soa/dir")
    # verifying that the method does not do in-place updates
    assert existing_config == existing_config_copy


def create_pod(name: str, labels: Dict[str, str]) -> V1Pod:
    """Helper function to create a mock V1Pod with given labels."""
    metadata = V1ObjectMeta(name=name, labels=labels)
    return V1Pod(metadata=metadata)


def test_group_pods_by_service_instance():
    pod1 = create_pod(
        "pod1",
        {
            "paasta.yelp.com/service": "serviceA",
            "paasta.yelp.com/instance": "instance1",
        },
    )
    pod2 = create_pod(
        "pod2",
        {
            "paasta.yelp.com/service": "serviceA",
            "paasta.yelp.com/instance": "instance1",
        },
    )
    pod3 = create_pod(
        "pod3",
        {
            "paasta.yelp.com/service": "serviceA",
            "paasta.yelp.com/instance": "instance2",
        },
    )
    pod4 = create_pod(
        "pod4",
        {
            "paasta.yelp.com/service": "serviceB",
            "paasta.yelp.com/instance": "instance1",
        },
    )

    pods: List[V1Pod] = [pod1, pod2, pod3, pod4]

    expected_output = {
        "serviceA": {
            "instance1": [pod1, pod2],
            "instance2": [pod3],
        },
        "serviceB": {
            "instance1": [pod4],
        },
    }

    assert group_pods_by_service_instance(pods) == expected_output


def test_group_pods_by_service_instance_with_missing_labels():
    # Create pods with missing labels
    pod1 = create_pod(
        "pod1", {"paasta.yelp.com/service": "serviceA"}
    )  # Missing instance
    pod2 = create_pod(
        "pod2", {"paasta.yelp.com/instance": "instance1"}
    )  # Missing service
    pod3 = create_pod("pod3", {})  # No labels at all

    pods: List[V1Pod] = [pod1, pod2, pod3]

    # Since none of the pods have both required labels, the result should be an empty dictionary
    assert group_pods_by_service_instance(pods) == {}


def test_group_pods_by_service_instance_with_no_pods():
    # Empty list of pods
    assert group_pods_by_service_instance([]) == {}


def test_group_pods_by_service_instance_with_none_labels():
    # Pod with metadata.labels set to None
    pod1 = V1Pod(metadata=V1ObjectMeta(name="pod1", labels=None))

    assert group_pods_by_service_instance([pod1]) == {}
