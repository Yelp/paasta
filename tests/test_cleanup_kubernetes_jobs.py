#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
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
from copy import deepcopy

import mock
import pytest
from kubernetes.client import V1Deployment
from kubernetes.client import V1StatefulSet
from pytest import fixture
from pytest import raises

from paasta_tools.cleanup_kubernetes_jobs import cleanup_unused_apps
from paasta_tools.cleanup_kubernetes_jobs import DontKillEverythingError
from paasta_tools.cleanup_kubernetes_jobs import main
from paasta_tools.eks_tools import EksDeploymentConfig
from paasta_tools.kubernetes.application.controller_wrappers import DeploymentWrapper
from paasta_tools.kubernetes.application.controller_wrappers import StatefulSetWrapper
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig


@fixture
def fake_deployment():
    fake_deployment = V1Deployment(
        metadata=mock.Mock(
            namespace="paastasvc-service",
            labels={
                "yelp.com/paasta_service": "service",
                "yelp.com/paasta_instance": "instance-1",
                "yelp.com/paasta_git_sha": "1234",
                "yelp.com/paasta_config_sha": "1234",
                "paasta.yelp.com/service": "service",
                "paasta.yelp.com/instance": "instance-1",
                "paasta.yelp.com/git_sha": "1234",
                "paasta.yelp.com/config_sha": "1234",
                "paasta.yelp.com/managed": "true",
            },
        ),
        spec=mock.Mock(replicas=0),
        status=mock.Mock(ready_replicas=0),
    )
    type(fake_deployment.metadata).name = mock.PropertyMock(
        return_value="service-instance-1"
    )
    return fake_deployment


@fixture
def fake_stateful_set():
    fake_stateful_set = V1StatefulSet(
        metadata=mock.Mock(
            namespace="paasta",
            labels={
                "yelp.com/paasta_service": "service",
                "yelp.com/paasta_instance": "instance-2",
                "yelp.com/paasta_git_sha": "1234",
                "yelp.com/paasta_config_sha": "1234",
                "paasta.yelp.com/service": "service",
                "paasta.yelp.com/instance": "instance-2",
                "paasta.yelp.com/git_sha": "1234",
                "paasta.yelp.com/config_sha": "1234",
                "paasta.yelp.com/managed": "true",
            },
        ),
        spec=mock.Mock(replicas=0),
    )
    type(fake_stateful_set.metadata).name = (
        mock.PropertyMock(return_value="service-instance-2"),
    )
    return fake_stateful_set


@fixture
def invalid_app():
    invalid_app = V1Deployment(
        metadata=mock.Mock(namespace="paasta", labels={}), spec=mock.Mock(replicas=0)
    )
    type(invalid_app.metadata).name = (mock.PropertyMock(return_value="invalid_app"),)
    return invalid_app


def fake_instance_config(
    cluster, service, instance, soa_dir="soa_dir", load_deployments=False
):
    fake_instance_config = KubernetesDeploymentConfig(
        service,
        instance,
        cluster,
        {
            "port": None,
            "monitoring": {},
            "deploy": {"pipeline": [{"step": "default"}]},
            "data": {},
            "smartstack": {},
            "dependencies": {},
            "cpus": 0.1,
            "mem": 100,
            "min_instances": 1,
            "max_instances": 10,
            "deploy_group": "prod.main",
            "autoscaling": {"setpoint": 0.7},
        },
        {
            "docker_image": "services-compute-infra-test-service:paasta-5b861b3bd42ef9674d3ca04a1259c79eddb71694",
            "git_sha": "5b861b3bd42ef9674d3ca04a1259c79eddb71694",
            "image_version": None,
            "desired_state": "start",
            "force_bounce": None,
        },
        soa_dir,
    )
    return fake_instance_config


def fake_eks_instance_config(
    cluster, service, instance, soa_dir="soa_dir", load_deployments=False
):
    fake_eks_instance_config = EksDeploymentConfig(
        service,
        instance,
        cluster,
        {
            "port": None,
            "monitoring": {},
            "deploy": {"pipeline": [{"step": "default"}]},
            "data": {},
            "smartstack": {},
            "dependencies": {},
            "cpus": 0.1,
            "mem": 100,
            "min_instances": 1,
            "max_instances": 10,
            "deploy_group": "prod.main",
            "autoscaling": {"setpoint": 0.7},
        },
        {
            "docker_image": "services-compute-infra-test-service:paasta-5b861b3bd42ef9674d3ca04a1259c79eddb71694",
            "git_sha": "5b861b3bd42ef9674d3ca04a1259c79eddb71694",
            "image_version": None,
            "desired_state": "start",
            "force_bounce": None,
        },
        soa_dir,
    )
    return fake_eks_instance_config


def get_fake_instances(self, with_limit: bool = True) -> int:
    return self.config_dict.get("max_instances", None)


def test_main(fake_deployment, fake_stateful_set, invalid_app):
    soa_dir = "paasta_maaaachine"
    cluster = "maaaachine_cluster"
    with mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.cleanup_unused_apps", autospec=True
    ) as cleanup_patch, mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.load_system_paasta_config", autospec=True
    ) as load_config_patch:
        load_config_patch.return_value.get_cluster.return_value = "fake_cluster"
        main(("--soa-dir", soa_dir, "--cluster", cluster))
        cleanup_patch.assert_called_once_with(
            soa_dir, cluster, kill_threshold=0.5, force=False, eks=False
        )


def test_list_apps(fake_deployment, fake_stateful_set, invalid_app):
    mock_kube_client = mock.MagicMock()
    with mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.KubeClient",
        return_value=mock_kube_client,
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.get_services_for_cluster",
        return_value={},
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.alert_state_change", autospec=True
    ) as mock_alert_state_change:
        mock_alert_state_change.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mock_alert_state_change.__exit__ = mock.Mock(return_value=None)
        cleanup_unused_apps("soa_dir", "fake cluster", kill_threshold=1, force=False)
        assert (
            mock_kube_client.deployments.list_deployment_for_all_namespaces.call_count
            == 1
        )
        assert (
            mock_kube_client.deployments.list_stateful_set_for_all_namespaces.call_count
            == 1
        )


@pytest.mark.parametrize(
    "eks_flag",
    [
        (False),
        (True),
    ],
)
def test_cleanup_unused_apps(eks_flag, fake_deployment, fake_stateful_set, invalid_app):
    mock_kube_client = mock.MagicMock()
    with mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.KubeClient",
        return_value=mock_kube_client,
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.list_all_applications",
        return_value={("service", "instance-1"): [DeploymentWrapper(fake_deployment)]},
        autospec=True,
    ), mock.patch(
        "paasta_tools.kubernetes_tools.load_kubernetes_service_config_no_cache",
        autospec=True,
        side_effect=fake_instance_config,
    ), mock.patch(
        "paasta_tools.eks_tools.load_eks_service_config_no_cache",
        autospec=True,
        side_effect=fake_eks_instance_config,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.KubernetesDeploymentConfig.get_instances",
        side_effect=get_fake_instances,
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.get_services_for_cluster",
        return_value={},
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.alert_state_change", autospec=True
    ) as mock_alert_state_change:
        mock_alert_state_change.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mock_alert_state_change.__exit__ = mock.Mock(return_value=None)
        cleanup_unused_apps(
            "soa_dir", "fake cluster", kill_threshold=1, force=False, eks=eks_flag
        )
        assert mock_kube_client.deployments.delete_namespaced_deployment.call_count == 1


@pytest.mark.parametrize(
    "eks_flag",
    [
        (False),
        (True),
    ],
)
def test_cleanup_unused_apps_in_multiple_namespaces(
    eks_flag, fake_deployment, fake_stateful_set, invalid_app
):
    mock_kube_client = mock.MagicMock()
    fake_deployment2 = deepcopy(fake_deployment)
    fake_deployment2.metadata.namespace = "paastasvc-blah"
    fake_deployment.status.ready_replicas = 10
    fake_deployment2.status.ready_replicas = 3
    with mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.KubeClient",
        return_value=mock_kube_client,
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.list_all_applications",
        return_value={
            ("service", "instance-1"): [
                DeploymentWrapper(fake_deployment),
                DeploymentWrapper(fake_deployment2),
            ]
        },
        autospec=True,
    ), mock.patch(
        "paasta_tools.kubernetes_tools.load_kubernetes_service_config_no_cache",
        autospec=True,
        side_effect=fake_instance_config,
    ), mock.patch(
        "paasta_tools.eks_tools.load_eks_service_config_no_cache",
        autospec=True,
        side_effect=fake_eks_instance_config,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.get_services_for_cluster",
        return_value={("service", "instance-1")},
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.KubernetesDeploymentConfig.get_instances",
        side_effect=get_fake_instances,
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.alert_state_change", autospec=True
    ) as mock_alert_state_change:
        mock_alert_state_change.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mock_alert_state_change.__exit__ = mock.Mock(return_value=None)
        cleanup_unused_apps(
            "soa_dir", "fake cluster", kill_threshold=2, force=False, eks=eks_flag
        )
        assert mock_kube_client.deployments.delete_namespaced_deployment.call_count == 1


@pytest.mark.parametrize(
    "eks_flag",
    [
        (False),
        (True),
    ],
)
def test_cleanup_unused_apps_does_not_delete(
    eks_flag, fake_deployment, fake_stateful_set, invalid_app
):
    mock_kube_client = mock.MagicMock()
    with mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.KubeClient",
        return_value=mock_kube_client,
        autospec=True,
    ), mock.patch(
        "paasta_tools.kubernetes_tools.load_kubernetes_service_config_no_cache",
        autospec=True,
        side_effect=fake_instance_config,
    ), mock.patch(
        "paasta_tools.eks_tools.load_eks_service_config_no_cache",
        autospec=True,
        side_effect=fake_eks_instance_config,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.list_all_applications",
        return_value={("service", "instance-1"): [DeploymentWrapper(fake_deployment)]},
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.KubernetesDeploymentConfig.get_instances",
        side_effect=get_fake_instances,
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.get_services_for_cluster",
        return_value={("service", "instance-1"), ("service", "instance-2")},
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.alert_state_change", autospec=True
    ) as mock_alert_state_change:
        mock_alert_state_change.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mock_alert_state_change.__exit__ = mock.Mock(return_value=None)
        cleanup_unused_apps(
            "soa_dir", "fake cluster", kill_threshold=1, force=False, eks=eks_flag
        )
        assert mock_kube_client.deployments.delete_namespaced_deployment.call_count == 0


@pytest.mark.parametrize(
    "eks_flag",
    [
        (False),
        (True),
    ],
)
def test_cleanup_unused_apps_does_not_delete_bouncing_apps(
    eks_flag, fake_deployment, fake_stateful_set, invalid_app
):
    mock_kube_client = mock.MagicMock()
    fake_deployment2 = deepcopy(fake_deployment)
    fake_deployment2.status.ready_replicas = 3
    fake_deployment.status.ready_replicas = 10
    fake_deployment.metadata.namespace = "paastasvc-blah"
    with mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.KubeClient",
        return_value=mock_kube_client,
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.list_all_applications",
        return_value={
            ("service", "instance-1"): [
                DeploymentWrapper(fake_deployment),
                DeploymentWrapper(fake_deployment2),
            ]
        },
        autospec=True,
    ), mock.patch(
        "paasta_tools.kubernetes_tools.load_kubernetes_service_config_no_cache",
        autospec=True,
        side_effect=fake_instance_config,
    ), mock.patch(
        "paasta_tools.eks_tools.load_eks_service_config_no_cache",
        autospec=True,
        side_effect=fake_eks_instance_config,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.get_services_for_cluster",
        return_value={("service", "instance-1")},
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.KubernetesDeploymentConfig.get_instances",
        side_effect=get_fake_instances,
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.alert_state_change", autospec=True
    ) as mock_alert_state_change:
        mock_alert_state_change.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mock_alert_state_change.__exit__ = mock.Mock(return_value=None)
        cleanup_unused_apps(
            "soa_dir", "fake cluster", kill_threshold=2, force=False, eks=eks_flag
        )
        assert mock_kube_client.deployments.delete_namespaced_deployment.call_count == 0


@pytest.mark.parametrize(
    "eks_flag",
    [
        (False),
        (True),
    ],
)
def test_cleanup_unused_apps_does_not_delete_recently_created_apps(
    eks_flag, fake_deployment, fake_stateful_set, invalid_app
):
    mock_kube_client = mock.MagicMock()
    fake_deployment.status.ready_replicas = 10
    fake_deployment.metadata.namespace = "paastasvc-blah"
    with mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.KubeClient",
        return_value=mock_kube_client,
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.list_all_applications",
        return_value={
            ("service", "instance-1"): [
                DeploymentWrapper(fake_deployment),
            ]
        },
        autospec=True,
    ), mock.patch(
        "paasta_tools.kubernetes_tools.load_kubernetes_service_config_no_cache",
        autospec=True,
        side_effect=fake_instance_config,
    ), mock.patch(
        "paasta_tools.eks_tools.load_eks_service_config_no_cache",
        autospec=True,
        side_effect=fake_eks_instance_config,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.get_services_for_cluster",
        return_value={("service", "instance-1")},
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.KubernetesDeploymentConfig.get_instances",
        side_effect=get_fake_instances,
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.alert_state_change", autospec=True
    ) as mock_alert_state_change:
        mock_alert_state_change.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mock_alert_state_change.__exit__ = mock.Mock(return_value=None)
        cleanup_unused_apps(
            "soa_dir", "fake cluster", kill_threshold=2, force=False, eks=eks_flag
        )
        assert mock_kube_client.deployments.delete_namespaced_deployment.call_count == 0


@pytest.mark.parametrize(
    "eks_flag",
    [
        (False),
        (True),
    ],
)
def test_cleanup_unused_apps_dont_kill_everything(
    eks_flag, fake_deployment, fake_stateful_set, invalid_app
):
    mock_kube_client = mock.MagicMock()
    with mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.KubeClient",
        return_value=mock_kube_client,
        autospec=True,
    ), mock.patch(
        "paasta_tools.kubernetes_tools.load_kubernetes_service_config_no_cache",
        autospec=True,
        side_effect=fake_instance_config,
    ), mock.patch(
        "paasta_tools.eks_tools.load_eks_service_config_no_cache",
        autospec=True,
        side_effect=fake_eks_instance_config,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.list_all_applications",
        return_value={("service", "instance-1"): [DeploymentWrapper(fake_deployment)]},
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.KubernetesDeploymentConfig.get_instances",
        side_effect=get_fake_instances,
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.get_services_for_cluster",
        return_value={},
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.alert_state_change", autospec=True
    ) as mock_alert_state_change:
        mock_alert_state_change.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mock_alert_state_change.__exit__ = mock.Mock(return_value=None)
        with raises(DontKillEverythingError):
            cleanup_unused_apps(
                "soa_dir", "fake_cluster", kill_threshold=0, force=False, eks=eks_flag
            )
        assert mock_kube_client.deployments.delete_namespaced_deployment.call_count == 0


@pytest.mark.parametrize(
    "eks_flag",
    [
        (False),
        (True),
    ],
)
def test_cleanup_unused_apps_dont_kill_statefulsets(
    eks_flag, fake_deployment, fake_stateful_set, invalid_app
):
    mock_kube_client = mock.MagicMock()
    with mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.KubeClient",
        return_value=mock_kube_client,
        autospec=True,
    ), mock.patch(
        "paasta_tools.kubernetes_tools.load_kubernetes_service_config_no_cache",
        autospec=True,
        side_effect=fake_instance_config,
    ), mock.patch(
        "paasta_tools.eks_tools.load_eks_service_config_no_cache",
        autospec=True,
        side_effect=fake_eks_instance_config,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.list_all_applications",
        return_value={
            ("service", "instance-2"): [
                StatefulSetWrapper(fake_stateful_set),
                StatefulSetWrapper(fake_stateful_set),
            ]
        },
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.get_services_for_cluster",
        return_value={("service", "instance-2")},
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.alert_state_change", autospec=True
    ) as mock_alert_state_change:
        mock_alert_state_change.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mock_alert_state_change.__exit__ = mock.Mock(return_value=None)
        cleanup_unused_apps(
            "soa_dir", "fake_cluster", kill_threshold=0.5, force=False, eks=eks_flag
        )
        assert mock_kube_client.deployments.delete_namespaced_deployment.call_count == 0


@pytest.mark.parametrize(
    "eks_flag",
    [
        (False),
        (True),
    ],
)
def test_cleanup_unused_apps_force(
    eks_flag, fake_deployment, fake_stateful_set, invalid_app
):
    mock_kube_client = mock.MagicMock()
    with mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.KubeClient",
        return_value=mock_kube_client,
        autospec=True,
    ), mock.patch(
        "paasta_tools.kubernetes_tools.load_kubernetes_service_config_no_cache",
        autospec=True,
        side_effect=fake_instance_config,
    ), mock.patch(
        "paasta_tools.eks_tools.load_eks_service_config_no_cache",
        autospec=True,
        side_effect=fake_eks_instance_config,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.list_all_applications",
        return_value={("service", "instance-1"): [DeploymentWrapper(fake_deployment)]},
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.KubernetesDeploymentConfig.get_instances",
        side_effect=get_fake_instances,
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.get_services_for_cluster",
        return_value={},
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.alert_state_change", autospec=True
    ) as mock_alert_state_change:
        mock_alert_state_change.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mock_alert_state_change.__exit__ = mock.Mock(return_value=None)
        cleanup_unused_apps(
            "soa_dir", "fake_cluster", kill_threshold=0, force=True, eks=eks_flag
        )
        assert mock_kube_client.deployments.delete_namespaced_deployment.call_count == 1


@pytest.mark.parametrize(
    "eks_flag",
    [
        (False),
        (True),
    ],
)
def test_cleanup_unused_apps_ignore_invalid_apps(
    eks_flag, fake_deployment, fake_stateful_set, invalid_app
):
    mock_kube_client = mock.MagicMock()
    with mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.KubeClient",
        return_value=mock_kube_client,
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.get_services_for_cluster",
        return_value={},
        autospec=True,
    ), mock.patch(
        "paasta_tools.cleanup_kubernetes_jobs.alert_state_change", autospec=True
    ) as mock_alert_state_change:
        mock_alert_state_change.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
        mock_alert_state_change.__exit__ = mock.Mock(return_value=None)
        mock_kube_client.deployments.list_namespaced_deployment.return_value = (
            mock.MagicMock(items=[invalid_app])
        )
        cleanup_unused_apps(
            "soa_dir", "fake_cluster", kill_threshold=0, force=True, eks=eks_flag
        )
        assert mock_kube_client.deployments.delete_namespaced_deployment.call_count == 0
