from typing import Any
from typing import Dict
from typing import Sequence

import mock
import pytest
from hypothesis import given
from hypothesis.strategies import floats
from hypothesis.strategies import integers
from kubernetes.client import V1Affinity
from kubernetes.client import V1AWSElasticBlockStoreVolumeSource
from kubernetes.client import V1beta1PodDisruptionBudget
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
from kubernetes.client import V1Handler
from kubernetes.client import V1HostPathVolumeSource
from kubernetes.client import V1HTTPGetAction
from kubernetes.client import V1LabelSelector
from kubernetes.client import V1Lifecycle
from kubernetes.client import V1NodeAffinity
from kubernetes.client import V1NodeSelector
from kubernetes.client import V1NodeSelectorRequirement
from kubernetes.client import V1NodeSelectorTerm
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1PersistentVolumeClaim
from kubernetes.client import V1PersistentVolumeClaimSpec
from kubernetes.client import V1PodSpec
from kubernetes.client import V1PodTemplateSpec
from kubernetes.client import V1Probe
from kubernetes.client import V1ResourceRequirements
from kubernetes.client import V1RollingUpdateDeployment
from kubernetes.client import V1SecretKeySelector
from kubernetes.client import V1SecurityContext
from kubernetes.client import V1StatefulSet
from kubernetes.client import V1StatefulSetSpec
from kubernetes.client import V1TCPSocketAction
from kubernetes.client import V1Volume
from kubernetes.client import V1VolumeMount
from kubernetes.client import V2beta1CrossVersionObjectReference
from kubernetes.client import V2beta1ExternalMetricSource
from kubernetes.client import V2beta1HorizontalPodAutoscaler
from kubernetes.client import V2beta1HorizontalPodAutoscalerSpec
from kubernetes.client import V2beta1MetricSpec
from kubernetes.client import V2beta1PodsMetricSource
from kubernetes.client import V2beta1ResourceMetricSource
from kubernetes.client.rest import ApiException

from paasta_tools import kubernetes_tools
from paasta_tools.kubernetes_tools import create_custom_resource
from paasta_tools.kubernetes_tools import create_deployment
from paasta_tools.kubernetes_tools import create_kubernetes_secret_signature
from paasta_tools.kubernetes_tools import create_pod_disruption_budget
from paasta_tools.kubernetes_tools import create_secret
from paasta_tools.kubernetes_tools import create_stateful_set
from paasta_tools.kubernetes_tools import ensure_namespace
from paasta_tools.kubernetes_tools import filter_nodes_by_blacklist
from paasta_tools.kubernetes_tools import filter_pods_by_service_instance
from paasta_tools.kubernetes_tools import force_delete_pods
from paasta_tools.kubernetes_tools import get_active_shas_for_service
from paasta_tools.kubernetes_tools import get_all_nodes
from paasta_tools.kubernetes_tools import get_all_pods
from paasta_tools.kubernetes_tools import get_kubernetes_app_by_name
from paasta_tools.kubernetes_tools import get_kubernetes_app_deploy_status
from paasta_tools.kubernetes_tools import get_kubernetes_secret_hashes
from paasta_tools.kubernetes_tools import get_kubernetes_secret_signature
from paasta_tools.kubernetes_tools import get_kubernetes_services_running_here
from paasta_tools.kubernetes_tools import get_kubernetes_services_running_here_for_nerve
from paasta_tools.kubernetes_tools import get_nodes_grouped_by_attribute
from paasta_tools.kubernetes_tools import InvalidKubernetesConfig
from paasta_tools.kubernetes_tools import is_node_ready
from paasta_tools.kubernetes_tools import is_pod_ready
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import KubeContainerResources
from paasta_tools.kubernetes_tools import KubeCustomResource
from paasta_tools.kubernetes_tools import KubeDeployment
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfigDict
from paasta_tools.kubernetes_tools import KubernetesDeployStatus
from paasta_tools.kubernetes_tools import KubeService
from paasta_tools.kubernetes_tools import list_all_deployments
from paasta_tools.kubernetes_tools import list_custom_resources
from paasta_tools.kubernetes_tools import load_kubernetes_service_config
from paasta_tools.kubernetes_tools import load_kubernetes_service_config_no_cache
from paasta_tools.kubernetes_tools import max_unavailable
from paasta_tools.kubernetes_tools import paasta_prefixed
from paasta_tools.kubernetes_tools import pod_disruption_budget_for_service_instance
from paasta_tools.kubernetes_tools import pods_for_service_instance
from paasta_tools.kubernetes_tools import sanitise_kubernetes_name
from paasta_tools.kubernetes_tools import set_instances_for_kubernetes_service
from paasta_tools.kubernetes_tools import update_custom_resource
from paasta_tools.kubernetes_tools import update_deployment
from paasta_tools.kubernetes_tools import update_kubernetes_secret_signature
from paasta_tools.kubernetes_tools import update_secret
from paasta_tools.kubernetes_tools import update_stateful_set
from paasta_tools.secret_tools import SHARED_SECRET_SERVICE
from paasta_tools.utils import AwsEbsVolume
from paasta_tools.utils import DockerVolume
from paasta_tools.utils import PersistentVolume
from paasta_tools.utils import SystemPaastaConfig


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
    def setup_method(self, method):
        hpa_config = {
            "min_replicas": 1,
            "max_replicas": 3,
            "cpu": {"target_average_value": 0.7},
            "memory": {"target_average_value": 0.7},
            "uwsgi": {"target_average_value": 0.7},
            "http": {"target_average_value": 0.7, "dimensions": {"any": "random"}},
            "external": {"target_value": 0.7, "signalflow_metrics_query": "fake_query"},
        }
        mock_config_dict = KubernetesDeploymentConfigDict(
            bounce_method="crossover", instances=3, horizontal_autoscaling=hpa_config,
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

    def test_get_deployment_strategy(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_bounce_method",
            autospec=True,
            return_value="crossover",
        ) as mock_get_bounce_method:
            assert self.deployment.get_deployment_strategy_config() == V1DeploymentStrategy(
                type="RollingUpdate",
                rolling_update=V1RollingUpdateDeployment(
                    max_surge="100%", max_unavailable="0%"
                ),
            )
            mock_get_bounce_method.return_value = "downthenup"
            assert self.deployment.get_deployment_strategy_config() == V1DeploymentStrategy(
                type="Recreate"
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

    def test_get_sidecar_containers(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_registrations",
            autospec=True,
            return_value=["universal.credit"],
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_kubernetes_environment",
            autospec=True,
            return_value={},
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_sanitised_volume_name",
            autospec=True,
            return_value="sane-name",
        ), mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_enable_nerve_readiness_check",
            autospec=True,
            return_value=False,
        ) as mock_get_enable_nerve_readiness_check:
            mock_system_config = mock.Mock(
                get_nerve_readiness_check_script=mock.Mock(
                    return_value="/nail/blah.sh"
                ),
                get_hacheck_sidecar_image_url=mock.Mock(
                    return_value="some-docker-image"
                ),
            )
            mock_service_namespace = mock.Mock(
                is_in_smartstack=mock.Mock(return_value=False)
            )

            assert (
                self.deployment.get_sidecar_containers(
                    mock_system_config, mock_service_namespace
                )
                == []
            )

            mock_service_namespace = mock.Mock(
                is_in_smartstack=mock.Mock(return_value=True)
            )

            ret = self.deployment.get_sidecar_containers(
                mock_system_config, mock_service_namespace
            )
            expected = [
                V1Container(
                    env={},
                    image="some-docker-image",
                    lifecycle=V1Lifecycle(
                        pre_stop=V1Handler(
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
                )
            ]
            assert ret == expected
            mock_get_enable_nerve_readiness_check.return_value = True
            mock_system_config = mock.Mock(
                get_nerve_readiness_check_script=mock.Mock(
                    return_value="/nail/blah.sh"
                ),
                get_hacheck_sidecar_image_url=mock.Mock(
                    return_value="some-docker-image"
                ),
            )
            ret = self.deployment.get_sidecar_containers(
                mock_system_config, mock_service_namespace
            )
            expected = [
                V1Container(
                    env={},
                    image="some-docker-image",
                    lifecycle=V1Lifecycle(
                        pre_stop=V1Handler(
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
            assert self.deployment.get_resource_requirements() == V1ResourceRequirements(
                limits={"cpu": 1.3, "memory": "2048Mi", "ephemeral-storage": "4096Mi"},
                requests={
                    "cpu": 0.3,
                    "memory": "2048Mi",
                    "ephemeral-storage": "4096Mi",
                },
            )

    def test_get_kubernetes_containers(self):
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
        ):
            mock_system_config = mock.Mock()
            mock_docker_volumes: Sequence[DockerVolume] = []
            mock_aws_ebs_volumes: Sequence[AwsEbsVolume] = []
            expected = [
                V1Container(
                    args=mock_get_args.return_value,
                    command=mock_get_cmd.return_value,
                    env=mock_get_container_env.return_value,
                    resources=mock_get_resource_requirements.return_value,
                    image=mock_get_docker_url.return_value,
                    lifecycle=V1Lifecycle(
                        pre_stop=V1Handler(
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
                    name="fm",
                    ports=[V1ContainerPort(container_port=8888)],
                    volume_mounts=mock_get_volume_mounts.return_value,
                ),
                "mock_sidecar",
            ]
            service_namespace_config = mock.Mock()
            service_namespace_config.get_healthcheck_mode.return_value = "http"
            service_namespace_config.get_healthcheck_uri.return_value = "/status"
            assert (
                self.deployment.get_kubernetes_containers(
                    docker_volumes=mock_docker_volumes,
                    system_paasta_config=mock_system_config,
                    aws_ebs_volumes=mock_aws_ebs_volumes,
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

    def test_get_security_context_without_cap_add(self):
        assert self.deployment.get_security_context() is None

    def test_get_security_context_with_cap_add(self):
        self.deployment.config_dict["cap_add"] = ["SETGID"]
        expected_security_context = V1SecurityContext(
            capabilities=V1Capabilities(add=["SETGID"])
        )
        assert self.deployment.get_security_context() == expected_security_context

    def test_get_pod_volumes(self):
        mock_docker_volumes = [
            {"hostPath": "/nail/blah", "containerPath": "/nail/foo"},
            {"hostPath": "/nail/thing", "containerPath": "/nail/bar"},
        ]
        mock_aws_ebs_volumes = [
            {
                "volume_id": "vol-zzzzzzzzzzzzzzzzz",
                "fs_type": "ext4",
                "container_path": "/nail/qux",
            }
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
                aws_elastic_block_store=V1AWSElasticBlockStoreVolumeSource(
                    volume_id="vol-zzzzzzzzzzzzzzzzz", fs_type="ext4", read_only=False
                ),
                name="aws-ebs--vol-zzzzzzzzzzzzzzzzz",
            ),
        ]
        assert (
            self.deployment.get_pod_volumes(
                docker_volumes=mock_docker_volumes, aws_ebs_volumes=mock_aws_ebs_volumes
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
                {"hostPath": "/nail/blah", "containerPath": "/nail/foo"},
                {"hostPath": "/nail/thing", "containerPath": "/nail/bar", "mode": "RW"},
            ]
            mock_aws_ebs_volumes = [
                {
                    "volume_id": "vol-ZZZZZZZZZZZZZZZZZ",
                    "fs_type": "ext4",
                    "container_path": "/nail/qux",
                }
            ]
            mock_persistent_volumes = [{"container_path": "/blah", "mode": "RW"}]
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
            ]
            assert (
                self.deployment.get_volume_mounts(
                    docker_volumes=mock_docker_volumes,
                    aws_ebs_volumes=mock_aws_ebs_volumes,
                    persistent_volumes=mock_persistent_volumes,
                )
                == expected_volumes
            )

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
            mock_get_aws_ebs_volumes.return_value = []
            assert self.deployment.get_desired_instances() == 3

            mock_get_aws_ebs_volumes.return_value = ["some-ebs-vol"]
            with pytest.raises(Exception):
                self.deployment.get_desired_instances()

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
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_volumes",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_service",
        autospec=True,
    )
    @mock.patch(
        "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_instance",
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
        "paasta_tools.kubernetes_tools.load_service_namespace_config", autospec=True,
    )
    @pytest.mark.parametrize(
        "in_smtstk,routable_ip,node_affinity,spec_affinity",
        [
            (True, "true", None, {}),
            (False, "false", None, {}),
            # an affinity obj is only added if there is a node affinity
            (
                False,
                "false",
                "a_node_affinity",
                {"affinity": V1Affinity(node_affinity="a_node_affinity")},
            ),
        ],
    )
    def test_get_pod_template_spec(
        self,
        mock_load_service_namespace_config,
        mock_get_node_affinity,
        mock_get_pod_volumes,
        mock_get_kubernetes_containers,
        mock_get_instance,
        mock_get_service,
        mock_get_volumes,
        in_smtstk,
        routable_ip,
        node_affinity,
        spec_affinity,
    ):
        mock_service_namespace_config = mock.Mock()
        mock_load_service_namespace_config.return_value = mock_service_namespace_config
        mock_service_namespace_config.is_in_smartstack.return_value = in_smtstk
        mock_get_node_affinity.return_value = node_affinity

        ret = self.deployment.get_pod_template_spec(
            git_sha="aaaa123", system_paasta_config=mock.Mock()
        )

        assert mock_load_service_namespace_config.called
        assert mock_service_namespace_config.is_in_smartstack.called
        assert mock_get_pod_volumes.called
        assert mock_get_volumes.called
        pod_spec_kwargs = dict(
            service_account_name=None,
            containers=mock_get_kubernetes_containers.return_value,
            share_process_namespace=True,
            node_selector={"yelp.com/pool": "default"},
            restart_policy="Always",
            volumes=[],
        )
        pod_spec_kwargs.update(spec_affinity)
        assert ret == V1PodTemplateSpec(
            metadata=V1ObjectMeta(
                labels={
                    "yelp.com/paasta_git_sha": "aaaa123",
                    "yelp.com/paasta_instance": mock_get_instance.return_value,
                    "yelp.com/paasta_service": mock_get_service.return_value,
                    "paasta.yelp.com/git_sha": "aaaa123",
                    "paasta.yelp.com/instance": mock_get_instance.return_value,
                    "paasta.yelp.com/service": mock_get_service.return_value,
                },
                annotations={
                    "smartstack_registrations": '["kurupt.fm"]',
                    "paasta.yelp.com/routable_ip": routable_ip,
                    "hpa": '{"http": {"any": "random"}, "uwsgi": {}}',
                    "iam.amazonaws.com/role": "",
                },
            ),
            spec=V1PodSpec(**pod_spec_kwargs),
        )

    @pytest.mark.parametrize(
        "whitelist,blacklist,expected",
        [
            (  # whitelist only
                ("habitat", ["habitat_a", "habitat_b"]),
                [],
                [
                    V1NodeSelectorRequirement(
                        key="yelp.com/habitat",
                        operator="In",
                        values=["habitat_a", "habitat_b"],
                    ),
                ],
            ),
            (  # blacklist only
                None,
                [("habitat", "habitat_a"), ("habitat", "habitat_b")],
                [
                    V1NodeSelectorRequirement(
                        key="yelp.com/habitat", operator="NotIn", values=["habitat_a"]
                    ),
                    V1NodeSelectorRequirement(
                        key="yelp.com/habitat", operator="NotIn", values=["habitat_b"]
                    ),
                ],
            ),
            (  # whitelist and blacklist
                ("habitat", ["habitat_a", "habitat_b"]),
                [("region", "region_a"), ("habitat", "habitat_c")],
                [
                    V1NodeSelectorRequirement(
                        key="yelp.com/habitat",
                        operator="In",
                        values=["habitat_a", "habitat_b"],
                    ),
                    V1NodeSelectorRequirement(
                        key="yelp.com/region", operator="NotIn", values=["region_a"]
                    ),
                    V1NodeSelectorRequirement(
                        key="yelp.com/habitat", operator="NotIn", values=["habitat_c"]
                    ),
                ],
            ),
        ],
    )
    def test_get_node_affinity(self, whitelist, blacklist, expected):
        self.deployment.config_dict["deploy_whitelist"] = whitelist
        self.deployment.config_dict["deploy_blacklist"] = blacklist

        node_affinity = self.deployment.get_node_affinity()

        assert node_affinity == V1NodeAffinity(
            required_during_scheduling_ignored_during_execution=V1NodeSelector(
                node_selector_terms=[V1NodeSelectorTerm(match_expressions=expected,)],
            ),
        )

    def test_get_node_affinity_no_blacklist_or_whitelist(self):
        self.deployment.config_dict["deploy_whitelist"] = None
        self.deployment.config_dict["deploy_blacklist"] = []
        assert self.deployment.get_node_affinity() is None

    def test_get_kubernetes_metadata(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_service",
            autospec=True,
            return_value="kurupt",
        ) as mock_get_service, mock.patch(
            "paasta_tools.kubernetes_tools.KubernetesDeploymentConfig.get_instance",
            autospec=True,
            return_value="fm",
        ) as mock_get_instance:

            ret = self.deployment.get_kubernetes_metadata("aaa123")
            assert ret == V1ObjectMeta(
                labels={
                    "yelp.com/paasta_git_sha": "aaa123",
                    "yelp.com/paasta_instance": mock_get_instance.return_value,
                    "yelp.com/paasta_service": mock_get_service.return_value,
                    "paasta.yelp.com/git_sha": "aaa123",
                    "paasta.yelp.com/instance": mock_get_instance.return_value,
                    "paasta.yelp.com/service": mock_get_service.return_value,
                },
                name="kurupt-fm",
            )

    def test_get_hpa_metric_spec(self):
        config_dict = {
            "horizontal_autoscaling": {
                "min_replicas": 1,
                "max_replicas": 3,
                "cpu": {"target_average_value": 0.7},
                "memory": {"target_average_value": 0.7},
                "uwsgi": {"target_average_value": 0.7},
                "http": {"target_average_value": 0.7, "dimensions": {"any": "random"}},
                "external": {
                    "target_value": 0.7,
                    "signalflow_metrics_query": "fake_query",
                },
            }
        }
        mock_config = KubernetesDeploymentConfig(  # type: ignore
            service="service",
            cluster="cluster",
            instance="instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        return_value = KubernetesDeploymentConfig.get_autoscaling_metric_spec(
            mock_config, "fake_name", "cluster"
        )
        annotations = {
            "signalfx.com.custom.metrics": "",
            "signalfx.com.external.metric/external": "fake_query",
            "signalfx.com.external.metric/http": 'data("http", filter=filter("any", "random")).mean(by="paasta_yelp_com_instance").mean(over="15m").publish()',
        }
        expected_res = V2beta1HorizontalPodAutoscaler(
            kind="HorizontalPodAutoscaler",
            metadata=V1ObjectMeta(
                name="fake_name", namespace="paasta", annotations=annotations
            ),
            spec=V2beta1HorizontalPodAutoscalerSpec(
                max_replicas=3,
                min_replicas=1,
                metrics=[
                    V2beta1MetricSpec(
                        type="Resource",
                        resource=V2beta1ResourceMetricSource(
                            name="cpu", target_average_utilization=70
                        ),
                    ),
                    V2beta1MetricSpec(
                        type="Resource",
                        resource=V2beta1ResourceMetricSource(
                            name="memory", target_average_utilization=70
                        ),
                    ),
                    V2beta1MetricSpec(
                        type="Pods",
                        pods=V2beta1PodsMetricSource(
                            metric_name="uwsgi",
                            target_average_value=0.7,
                            selector=V1LabelSelector(
                                match_labels={"paasta_cluster": "cluster"}
                            ),
                        ),
                    ),
                    V2beta1MetricSpec(
                        type="External",
                        external=V2beta1ExternalMetricSource(
                            metric_name="http", target_value=0.7,
                        ),
                    ),
                    V2beta1MetricSpec(
                        type="External",
                        external=V2beta1ExternalMetricSource(
                            metric_name="external", target_value=0.7,
                        ),
                    ),
                ],
                scale_target_ref=V2beta1CrossVersionObjectReference(
                    api_version="apps/v1", kind="Deployment", name="fake_name",
                ),
            ),
        )
        assert expected_res == return_value

    def test_get_autoscaling_metric_spec_mesos_cpu(self):
        # with cpu
        config_dict = {
            "min_instances": 1,
            "max_instances": 3,
            "autoscaling": {"metrics_provider": "mesos_cpu", "setpoint": 0.5},
        }
        mock_config = KubernetesDeploymentConfig(  # type: ignore
            service="service",
            cluster="cluster",
            instance="instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        return_value = KubernetesDeploymentConfig.get_autoscaling_metric_spec(
            mock_config, "fake_name", "cluster"
        )
        annotations: Dict[Any, Any] = {}
        expected_res = V2beta1HorizontalPodAutoscaler(
            kind="HorizontalPodAutoscaler",
            metadata=V1ObjectMeta(
                name="fake_name", namespace="paasta", annotations=annotations
            ),
            spec=V2beta1HorizontalPodAutoscalerSpec(
                max_replicas=3,
                min_replicas=1,
                metrics=[
                    V2beta1MetricSpec(
                        type="Resource",
                        resource=V2beta1ResourceMetricSource(
                            name="cpu", target_average_utilization=50.0
                        ),
                    )
                ],
                scale_target_ref=V2beta1CrossVersionObjectReference(
                    api_version="apps/v1", kind="Deployment", name="fake_name",
                ),
            ),
        )
        assert expected_res == return_value

    def test_get_autoscaling_metric_spec_http(self):
        # with http
        config_dict = {
            "min_instances": 1,
            "max_instances": 3,
            "autoscaling": {"metrics_provider": "http", "setpoint": 0.5},
        }
        mock_config = KubernetesDeploymentConfig(  # type: ignore
            service="service",
            cluster="cluster",
            instance="instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        return_value = KubernetesDeploymentConfig.get_autoscaling_metric_spec(
            mock_config, "fake_name", "cluster"
        )
        annotations = {"signalfx.com.custom.metrics": ""}
        expected_res = V2beta1HorizontalPodAutoscaler(
            kind="HorizontalPodAutoscaler",
            metadata=V1ObjectMeta(
                name="fake_name", namespace="paasta", annotations=annotations
            ),
            spec=V2beta1HorizontalPodAutoscalerSpec(
                max_replicas=3,
                min_replicas=1,
                metrics=[
                    V2beta1MetricSpec(
                        type="Pods",
                        pods=V2beta1PodsMetricSource(
                            metric_name="http",
                            target_average_value=0.5,
                            selector=V1LabelSelector(
                                match_labels={"paasta_cluster": "cluster"}
                            ),
                        ),
                    )
                ],
                scale_target_ref=V2beta1CrossVersionObjectReference(
                    api_version="apps/v1", kind="Deployment", name="fake_name",
                ),
            ),
        )
        assert expected_res == return_value

    def test_get_autoscaling_metric_spec_uwsgi(self):
        config_dict = {
            "min_instances": 1,
            "max_instances": 3,
            "autoscaling": {"metrics_provider": "uwsgi", "setpoint": 0.5},
        }
        mock_config = KubernetesDeploymentConfig(  # type: ignore
            service="service",
            cluster="cluster",
            instance="instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        return_value = KubernetesDeploymentConfig.get_autoscaling_metric_spec(
            mock_config, "fake_name", "cluster"
        )

        annotations = {"signalfx.com.custom.metrics": ""}
        expected_res = V2beta1HorizontalPodAutoscaler(
            kind="HorizontalPodAutoscaler",
            metadata=V1ObjectMeta(
                name="fake_name", namespace="paasta", annotations=annotations
            ),
            spec=V2beta1HorizontalPodAutoscalerSpec(
                max_replicas=3,
                min_replicas=1,
                metrics=[
                    V2beta1MetricSpec(
                        type="Pods",
                        pods=V2beta1PodsMetricSource(
                            metric_name="uwsgi",
                            target_average_value=0.5,
                            selector=V1LabelSelector(
                                match_labels={"paasta_cluster": "cluster"}
                            ),
                        ),
                    )
                ],
                scale_target_ref=V2beta1CrossVersionObjectReference(
                    api_version="apps/v1", kind="Deployment", name="fake_name",
                ),
            ),
        )
        assert expected_res == return_value

    def test_get_autoscaling_metric_spec_bespoke(self):
        config_dict = {
            "min_instances": 1,
            "max_instances": 3,
            "autoscaling": {"metrics_provider": "bespoke", "setpoint": 0.5},
        }
        mock_config = KubernetesDeploymentConfig(  # type: ignore
            service="service",
            cluster="cluster",
            instance="instance",
            config_dict=config_dict,
            branch_dict=None,
        )
        return_value = KubernetesDeploymentConfig.get_autoscaling_metric_spec(
            mock_config, "fake_name", "cluster"
        )
        expected_res = None
        assert expected_res == return_value

    def test_sanitize_for_config_hash(self):
        with mock.patch(
            "paasta_tools.kubernetes_tools.get_kubernetes_secret_hashes", autospec=True
        ) as mock_get_kubernetes_secret_hashes:
            mock_config = V1Deployment(
                metadata=V1ObjectMeta(name="qwe", labels={"mc": "grindah"}),
                spec=V1DeploymentSpec(
                    replicas=2,
                    selector=V1LabelSelector(match_labels={"freq": "108.9"}),
                    template=V1PodTemplateSpec(),
                ),
            )
            ret = self.deployment.sanitize_for_config_hash(mock_config)
            assert "replicas" not in ret["spec"].keys()
            assert (
                ret["paasta_secrets"] == mock_get_kubernetes_secret_hashes.return_value
            )

    def test_get_kubernetes_secret_env_vars(self):
        assert self.deployment.get_kubernetes_secret_env_vars(
            secret_env_vars={"SOME": "SECRET(a_ref)"},
            shared_secret_env_vars={"A": "SHAREDSECRET(_ref1)"},
        ) == [
            V1EnvVar(
                name="SOME",
                value_from=V1EnvVarSource(
                    secret_key_ref=V1SecretKeySelector(
                        name="paasta-secret-kurupt-a--ref", key="a_ref", optional=False
                    )
                ),
            ),
            V1EnvVar(
                name="A",
                value_from=V1EnvVarSource(
                    secret_key_ref=V1SecretKeySelector(
                        name="paasta-secret-underscore-shared-underscore-ref1",
                        key="_ref1",
                        optional=False,
                    )
                ),
            ),
        ]

    def test_get_bounce_margin_factor(self):
        assert isinstance(self.deployment.get_bounce_margin_factor(), float)

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
        pv = kubernetes_tools.PersistentVolume()
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

    def test_get_storage_class_name_correct(self):
        for sc in ["ebs", "ebs-slow"]:
            pv = PersistentVolume(
                storage_class_name=sc, size=1000, container_path="/dev/null", mode="rw",
            )
            assert self.deployment.get_storage_class_name(pv) == sc

    def test_get_persistent_volume_name(self):
        pv_name = self.deployment.get_persistent_volume_name(
            {"container_path": "/blah/what"}
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

        mock_requests_get.return_value.json.return_value = {
            "items": [
                {
                    "status": {"phase": "Running", "podIP": "10.1.1.1"},
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
                {
                    "status": {"phase": "Something", "podIP": "10.1.1.1"},
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
        assert get_kubernetes_services_running_here() == [
            KubeService(
                name="kurupt",
                instance="fm",
                port=8888,
                pod_ip="10.1.1.1",
                registrations=[],
            )
        ]


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

        mock_load_service_namespace.side_effect = lambda service, namespace, soa_dir: MockNerveDict(
            name=namespace
        )
        mock_get_kubernetes_services_running_here.return_value = [
            KubeService(
                name="kurupt",
                instance="fm",
                port=8888,
                pod_ip="10.1.1.1",
                registrations=["kurupt.fm"],
            ),
            KubeService(
                name="unkurupt",
                instance="garage",
                port=8888,
                pod_ip="10.1.1.1",
                registrations=["unkurupt.garage"],
            ),
            KubeService(
                name="kurupt",
                instance="garage",
                port=8888,
                pod_ip="10.1.1.1",
                registrations=[],
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


def test_ensure_namespace():
    mock_metadata = mock.Mock()
    type(mock_metadata).name = "paasta"
    mock_namespaces = mock.Mock(items=[mock.Mock(metadata=mock_metadata)])
    mock_client = mock.Mock(
        core=mock.Mock(list_namespace=mock.Mock(return_value=mock_namespaces))
    )
    ensure_namespace(mock_client, namespace="paasta")
    assert not mock_client.core.create_namespace.called

    mock_metadata = mock.Mock()
    type(mock_metadata).name = "kube-system"
    mock_namespaces = mock.Mock(items=[mock.Mock(metadata=mock_metadata)])
    mock_client = mock.Mock(
        core=mock.Mock(list_namespace=mock.Mock(return_value=mock_namespaces))
    )
    ensure_namespace(mock_client, namespace="paasta")
    assert mock_client.core.create_namespace.called

    mock_client.core.create_namespace.reset_mock()
    mock_namespaces = mock.Mock(items=[])
    mock_client = mock.Mock(
        core=mock.Mock(list_namespace=mock.Mock(return_value=mock_namespaces))
    )
    ensure_namespace(mock_client, namespace="paasta")
    assert mock_client.core.create_namespace.called


def test_list_all_deployments():
    mock_deployments = mock.Mock(items=[])
    mock_stateful_sets = mock.Mock(items=[])
    mock_client = mock.Mock(
        deployments=mock.Mock(
            list_namespaced_deployment=mock.Mock(return_value=mock_deployments),
            list_namespaced_stateful_set=mock.Mock(return_value=mock_stateful_sets),
        )
    )
    assert list_all_deployments(mock_client) == []

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
                }
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
                }
            )
        ),
    ]
    type(mock_items[0]).spec = mock.Mock(replicas=3)
    type(mock_items[1]).spec = mock.Mock(replicas=3)
    mock_deployments = mock.Mock(items=[mock_items[0]])
    mock_stateful_sets = mock.Mock(items=[mock_items[1]])
    mock_client = mock.Mock(
        deployments=mock.Mock(
            list_namespaced_deployment=mock.Mock(return_value=mock_deployments),
            list_namespaced_stateful_set=mock.Mock(return_value=mock_stateful_sets),
        )
    )
    assert list_all_deployments(mock_client) == [
        KubeDeployment(
            service="kurupt",
            instance="fm",
            git_sha="a12345",
            config_sha="b12345",
            replicas=3,
        ),
        KubeDeployment(
            service="kurupt",
            instance="am",
            git_sha="a12345",
            config_sha="b12345",
            replicas=3,
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
    event_loop, pod_logs, container_name, term_error, expected,
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
            kube_client=kube_client, pod=pod, container=container, num_tail_lines=10,
        ),
    )

    assert tail_lines == expected
    assert kube_client.core.read_namespaced_pod_log.call_args_list == [
        mock.call(
            name="my--pod",
            namespace="my_namespace",
            container=container_name,
            tail_lines=10,
        ),
    ]


@given(integers(min_value=0), floats(min_value=0, max_value=1.0))
def test_max_unavailable(instances, bmf):
    res = max_unavailable(instances, bmf)
    if instances == 0:
        assert res == 0
    if instances > 0:
        assert res >= 1 and res <= instances
    assert type(res) is int


def test_pod_disruption_budget_for_service_instance():
    x = pod_disruption_budget_for_service_instance(
        service="foo_1", instance="bar_1", max_unavailable="10%"
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
    mock_pdr = V1beta1PodDisruptionBudget()
    create_pod_disruption_budget(mock_client, mock_pdr)
    mock_client.policy.create_namespaced_pod_disruption_budget.assert_called_with(
        namespace="paasta", body=mock_pdr
    )


def test_create_deployment():
    mock_client = mock.Mock()
    create_deployment(mock_client, V1Deployment(api_version="some"))
    mock_client.deployments.create_namespaced_deployment.assert_called_with(
        namespace="paasta", body=V1Deployment(api_version="some")
    )


def test_update_deployment():
    mock_client = mock.Mock()
    update_deployment(mock_client, V1Deployment(metadata=V1ObjectMeta(name="kurupt")))
    mock_client.deployments.replace_namespaced_deployment.assert_called_with(
        namespace="paasta",
        name="kurupt",
        body=V1Deployment(metadata=V1ObjectMeta(name="kurupt")),
    )

    mock_client = mock.Mock()
    create_deployment(mock_client, V1Deployment(api_version="some"))
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
    create_stateful_set(mock_client, V1StatefulSet(api_version="some"))
    mock_client.deployments.create_namespaced_stateful_set.assert_called_with(
        namespace="paasta", body=V1StatefulSet(api_version="some")
    )


def test_update_stateful_set():
    mock_client = mock.Mock()
    update_stateful_set(
        mock_client, V1StatefulSet(metadata=V1ObjectMeta(name="kurupt"))
    )
    mock_client.deployments.replace_namespaced_stateful_set.assert_called_with(
        namespace="paasta",
        name="kurupt",
        body=V1StatefulSet(metadata=V1ObjectMeta(name="kurupt")),
    )

    mock_client = mock.Mock()
    create_stateful_set(mock_client, V1StatefulSet(api_version="some"))
    mock_client.deployments.create_namespaced_stateful_set.assert_called_with(
        namespace="paasta", body=V1StatefulSet(api_version="some")
    )


def test_get_kubernetes_app_deploy_status():
    mock_status = mock.Mock(replicas=1, ready_replicas=1, updated_replicas=1)
    mock_app = mock.Mock(status=mock_status)
    assert (
        get_kubernetes_app_deploy_status(mock_app, desired_instances=1)
        == KubernetesDeployStatus.Running
    )

    assert (
        get_kubernetes_app_deploy_status(mock_app, desired_instances=2)
        == KubernetesDeployStatus.Waiting
    )

    mock_status = mock.Mock(replicas=1, ready_replicas=2, updated_replicas=1)
    mock_app = mock.Mock(status=mock_status)
    assert (
        get_kubernetes_app_deploy_status(mock_app, desired_instances=2)
        == KubernetesDeployStatus.Deploying
    )

    mock_status = mock.Mock(replicas=0, ready_replicas=0, updated_replicas=0)
    mock_app = mock.Mock(status=mock_status)
    assert (
        get_kubernetes_app_deploy_status(mock_app, desired_instances=0)
        == KubernetesDeployStatus.Stopped
    )

    mock_status = mock.Mock(replicas=1, ready_replicas=None, updated_replicas=None)
    mock_app = mock.Mock(status=mock_status)
    assert (
        get_kubernetes_app_deploy_status(mock_app, desired_instances=1)
        == KubernetesDeployStatus.Waiting
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
    assert get_kubernetes_app_by_name("someservice", mock_client) == mock_deployment
    assert mock_client.deployments.read_namespaced_deployment_status.called
    assert not mock_client.deployments.read_namespaced_stateful_set_status.called

    mock_stateful_set = mock.Mock()
    mock_client.deployments.read_namespaced_deployment_status.reset_mock()
    mock_client.deployments.read_namespaced_deployment_status.side_effect = ApiException(
        404
    )
    mock_client.deployments.read_namespaced_stateful_set_status.return_value = (
        mock_stateful_set
    )
    assert get_kubernetes_app_by_name("someservice", mock_client) == mock_stateful_set
    assert mock_client.deployments.read_namespaced_deployment_status.called
    assert mock_client.deployments.read_namespaced_stateful_set_status.called


def test_pods_for_service_instance():
    mock_client = mock.Mock()
    assert (
        pods_for_service_instance("kurupt", "fm", mock_client)
        == mock_client.core.list_namespaced_pod.return_value.items
    )


def test_get_active_shas_for_service():
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
    ]
    assert get_active_shas_for_service(mock_pod_list) == {
        "git_sha": {"b456", "b456!!!"},
        "config_sha": {"a123", "a123!!!"},
    }


def test_get_all_pods():
    mock_client = mock.Mock()
    assert (
        get_all_pods(mock_client)
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


def test_create_kubernetes_secret_signature():
    mock_client = mock.Mock()
    create_kubernetes_secret_signature(
        kube_client=mock_client,
        secret="mortys-fate",
        service="universe",
        secret_signature="ab1234",
    )
    assert mock_client.core.create_namespaced_config_map.called


def test_update_kubernetes_secret_signature():
    mock_client = mock.Mock()
    update_kubernetes_secret_signature(
        kube_client=mock_client,
        secret="mortys-fate",
        service="universe",
        secret_signature="ab1234",
    )
    assert mock_client.core.replace_namespaced_config_map.called


def test_get_kubernetes_secret_signature():
    mock_client = mock.Mock()
    mock_client.core.read_namespaced_config_map.return_value = mock.Mock(
        data={"signature": "hancock"}
    )
    assert (
        get_kubernetes_secret_signature(
            kube_client=mock_client, secret="mortys-morty", service="universe"
        )
        == "hancock"
    )
    mock_client.core.read_namespaced_config_map.side_effect = ApiException(404)
    assert (
        get_kubernetes_secret_signature(
            kube_client=mock_client, secret="mortys-morty", service="universe"
        )
        is None
    )
    mock_client.core.read_namespaced_config_map.side_effect = ApiException(401)
    with pytest.raises(ApiException):
        get_kubernetes_secret_signature(
            kube_client=mock_client, secret="mortys-morty", service="universe"
        )


def test_create_secret():
    mock_client = mock.Mock()
    mock_secret_provider = mock.Mock()
    mock_secret_provider.decrypt_secret_raw.return_value = bytes("plaintext", "utf-8")
    create_secret(
        kube_client=mock_client,
        service="universe",
        secret="mortys-fate",
        secret_provider=mock_secret_provider,
    )
    assert mock_client.core.create_namespaced_secret.called
    mock_secret_provider.decrypt_secret_raw.assert_called_with("mortys-fate")

    create_secret(
        kube_client=mock_client,
        service="universe",
        secret="mortys_fate",
        secret_provider=mock_secret_provider,
    )
    mock_secret_provider.decrypt_secret_raw.assert_called_with("mortys_fate")


def test_update_secret():
    mock_client = mock.Mock()
    mock_secret_provider = mock.Mock()
    mock_secret_provider.decrypt_secret_raw.return_value = bytes("plaintext", "utf-8")

    update_secret(
        kube_client=mock_client,
        service="universe",
        secret="mortys-fate",
        secret_provider=mock_secret_provider,
    )
    assert mock_client.core.replace_namespaced_secret.called
    mock_secret_provider.decrypt_secret_raw.assert_called_with("mortys-fate")

    update_secret(
        kube_client=mock_client,
        service="universe",
        secret="mortys_fate",
        secret_provider=mock_secret_provider,
    )
    mock_secret_provider.decrypt_secret_raw.assert_called_with("mortys_fate")


def test_get_kubernetes_secret_hashes():
    with mock.patch(
        "paasta_tools.kubernetes_tools.KubeClient", autospec=True
    ) as mock_client, mock.patch(
        "paasta_tools.kubernetes_tools.is_secret_ref", autospec=True
    ) as mock_is_secret_ref, mock.patch(
        "paasta_tools.kubernetes_tools.get_kubernetes_secret_signature",
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
        )
        mock_get_kubernetes_secret_signature.assert_has_calls(
            [
                mock.call(
                    kube_client=mock_client.return_value,
                    secret="ref",
                    service="universe",
                ),
                mock.call(
                    kube_client=mock_client.return_value,
                    secret="ref1",
                    service=SHARED_SECRET_SERVICE,
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


def test_warning_big_bounce():
    job_config = kubernetes_tools.KubernetesDeploymentConfig(
        service="service",
        instance="instance",
        cluster="cluster",
        config_dict={},
        branch_dict={
            "docker_image": "abcdef",
            "git_sha": "deadbeef",
            "force_bounce": None,
            "desired_state": "start",
        },
    )

    with mock.patch(
        "paasta_tools.utils.load_system_paasta_config",
        return_value=SystemPaastaConfig(
            {
                "volumes": [],
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
    ):
        assert (
            job_config.format_kubernetes_app().spec.template.metadata.labels[
                "paasta.yelp.com/config_sha"
            ]
            == "config3b06ff5f"
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
        ("a_node_name", mock.Mock(metadata=mock.Mock(labels={})), "a_node_name",),
        # ApiException, default to node_name
        ("a_node_name", ApiException(), "a_node_name",),
    ],
)
def test_get_pod_hostname(pod_node_name, node, expected):
    client = mock.MagicMock()
    client.core.read_node.side_effect = [node]
    pod = mock.MagicMock()
    pod.spec.node_name = pod_node_name

    hostname = kubernetes_tools.get_pod_hostname(client, pod)

    assert hostname == expected
