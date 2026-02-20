from unittest import mock

import pytest
from kubernetes.client import V1DeleteOptions
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes.application.controller_wrappers import Application
from paasta_tools.kubernetes.application.controller_wrappers import DeploymentWrapper
from paasta_tools.kubernetes.application.controller_wrappers import JobWrapper
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig


@pytest.fixture
def mock_pdr_for_service_instance():
    with mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.pod_disruption_budget_for_service_instance",
        autospec=True,
    ) as mock_pdr_for_service_instance:
        yield mock_pdr_for_service_instance


@pytest.fixture
def mock_load_system_paasta_config():
    with mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.load_system_paasta_config",
        autospec=True,
    ) as mock_load_system_paasta_config:
        yield mock_load_system_paasta_config


@pytest.mark.parametrize("bounce_margin_factor_set", [True, False])
def test_ensure_pod_disruption_budget_create(
    bounce_margin_factor_set,
    mock_pdr_for_service_instance,
    mock_load_system_paasta_config,
):
    mock_load_system_paasta_config.return_value.get_pdb_max_unavailable.return_value = 3

    mock_req_pdr = mock.Mock()
    mock_req_pdr.spec.max_unavailable = 10 if bounce_margin_factor_set else 3
    mock_pdr_for_service_instance.return_value = mock_req_pdr

    mock_client = mock.MagicMock()

    mock_client.policy.read_namespaced_pod_disruption_budget.side_effect = ApiException(
        status=404
    )

    app = mock.MagicMock()
    if bounce_margin_factor_set:
        app.soa_config.config_dict = {"bounce_margin_factor": 0.1}
        app.soa_config.get_bounce_margin_factor.return_value = 0.1
    app.kube_deployment.service.return_value = "fake_service"
    app.kube_deployment.instance.return_value = "fake_instance"
    Application.ensure_pod_disruption_budget(
        self=app, kube_client=mock_client, namespace="paasta"
    )
    mock_client.policy.create_namespaced_pod_disruption_budget.assert_called_once_with(
        body=mock_req_pdr, namespace=mock_req_pdr.metadata.namespace
    )


def test_ensure_pod_disruption_budget_replaces_outdated(
    mock_pdr_for_service_instance, mock_load_system_paasta_config
):
    mock_req_pdr = mock.Mock()
    mock_req_pdr.spec.max_unavailable = 10
    mock_pdr_for_service_instance.return_value = mock_req_pdr

    mock_client = mock.MagicMock()

    mock_pdr = mock.Mock()
    mock_pdr.spec.max_unavailable = 5
    mock_pdr.spec.min_available = None

    mock_client.policy.read_namespaced_pod_disruption_budget.return_value = mock_pdr

    app = mock.MagicMock()
    app.soa_config.get_bounce_margin_factor.return_value = 0.1
    app.kube_deployment.service.return_value = "fake_service"
    app.kube_deployment.instance.return_value = "fake_instance"
    Application.ensure_pod_disruption_budget(
        self=app, kube_client=mock_client, namespace="paasta"
    )

    mock_client.policy.patch_namespaced_pod_disruption_budget.assert_called_once_with(
        name=mock_req_pdr.metadata.name,
        namespace=mock_req_pdr.metadata.namespace,
        body=mock_req_pdr,
    )


def test_ensure_pod_disruption_budget_noop_when_min_available_is_set(
    mock_pdr_for_service_instance, mock_load_system_paasta_config
):
    mock_req_pdr = mock.Mock()
    mock_req_pdr.spec.max_unavailable = 10
    mock_pdr_for_service_instance.return_value = mock_req_pdr

    mock_client = mock.MagicMock()

    mock_pdr = mock.Mock()
    mock_pdr.spec.max_unavailable = 5
    mock_pdr.spec.min_available = 5

    mock_client.policy.read_namespaced_pod_disruption_budget.return_value = mock_pdr

    app = mock.MagicMock()
    app.soa_config.get_bounce_margin_factor.return_value = 0.1
    app.kube_deployment.service.return_value = "fake_service"
    app.kube_deployment.instance.return_value = "fake_instance"
    Application.ensure_pod_disruption_budget(
        self=app, kube_client=mock_client, namespace="paasta"
    )

    mock_client.policy.patch_namespaced_pod_disruption_budget.assert_not_called()


def setup_app(config_dict, exists_hpa):
    item = mock.MagicMock()
    item.metadata.name = "fake_name"
    item.metadata.namespace = "faasta"

    app = DeploymentWrapper(item=item)
    app.soa_config = KubernetesDeploymentConfig(
        service="service",
        cluster="cluster",
        instance="instance",
        config_dict=config_dict,
        branch_dict=None,
    )

    app.exists_hpa = mock.Mock(return_value=exists_hpa)
    app.delete_horizontal_pod_autoscaler = mock.Mock(return_value=None)
    return app


@mock.patch(
    "paasta_tools.kubernetes.application.controller_wrappers.autoscaling_is_paused",
    autospec=True,
)
def test_sync_horizontal_pod_autoscaler_no_autoscaling(mock_autoscaling_is_paused):
    mock_client = mock.MagicMock()
    # Do nothing
    config_dict = {"instances": 1}
    app = setup_app(config_dict, False)
    mock_autoscaling_is_paused.return_value = False
    assert (
        mock_client.autoscaling.create_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert (
        mock_client.autoscaling.replace_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert app.delete_horizontal_pod_autoscaler.call_count == 0


@mock.patch(
    "paasta_tools.kubernetes.application.controller_wrappers.autoscaling_is_paused",
    autospec=True,
)
def test_sync_horizontal_pod_autoscaler_delete_hpa_when_no_autoscaling(
    mock_autoscaling_is_paused,
):
    mock_client = mock.MagicMock()
    # old HPA got removed so delete
    config_dict = {"instances": 1}
    app = setup_app(config_dict, True)

    mock_autoscaling_is_paused.return_value = False
    app.sync_horizontal_pod_autoscaler(kube_client=mock_client)
    assert (
        mock_client.autoscaling.create_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert (
        mock_client.autoscaling.replace_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert app.delete_horizontal_pod_autoscaler.call_count == 1


@mock.patch(
    "paasta_tools.kubernetes.application.controller_wrappers.autoscaling_is_paused",
    autospec=True,
)
def test_sync_horizontal_pod_autoscaler_when_autoscaling_is_paused(
    mock_autoscaling_is_paused,
):
    mock_client = mock.MagicMock()
    config_dict = {"max_instances": 3, "min_instances": 1}
    app = setup_app(config_dict, True)
    app.item.spec.replicas = 2

    mock_autoscaling_is_paused.return_value = True
    app.sync_horizontal_pod_autoscaler(kube_client=mock_client)
    assert (
        mock_client.autoscaling.create_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert (
        mock_client.autoscaling.replace_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert app.delete_horizontal_pod_autoscaler.call_count == 1


@mock.patch(
    "paasta_tools.kubernetes.application.controller_wrappers.autoscaling_is_paused",
    autospec=True,
)
def test_sync_horizontal_pod_autoscaler_when_autoscaling_is_resumed(
    mock_autoscaling_is_paused,
):
    mock_client = mock.MagicMock()
    config_dict = {"max_instances": 3, "min_instances": 1}
    app = setup_app(config_dict, True)
    app.item.spec.replicas = 2

    mock_autoscaling_is_paused.return_value = False
    app.sync_horizontal_pod_autoscaler(kube_client=mock_client)
    assert (
        mock_client.autoscaling.create_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert (
        mock_client.autoscaling.replace_namespaced_horizontal_pod_autoscaler.call_count
        == 1
    )


@mock.patch(
    "paasta_tools.kubernetes.application.controller_wrappers.autoscaling_is_paused",
    autospec=True,
)
def test_sync_horizontal_pod_autoscaler_create_hpa(mock_autoscaling_is_paused):
    mock_client = mock.MagicMock()
    # Create
    config_dict = {"max_instances": 3}
    app = setup_app(config_dict, False)

    mock_autoscaling_is_paused.return_value = False
    app.sync_horizontal_pod_autoscaler(kube_client=mock_client)

    assert (
        mock_client.autoscaling.replace_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert app.delete_horizontal_pod_autoscaler.call_count == 0
    mock_client.autoscaling.create_namespaced_horizontal_pod_autoscaler.assert_called_once_with(
        namespace="faasta",
        body=app.soa_config.get_autoscaling_metric_spec(
            "fake_name",
            "cluster",
            mock_client,
            namespace="faasta",
        ),
        pretty=True,
    )


@mock.patch(
    "paasta_tools.kubernetes.application.controller_wrappers.autoscaling_is_paused",
    autospec=True,
    return_value=False,
)
def test_sync_horizontal_pod_autoscaler_do_not_create_hpa_bespoke(
    mock_autoscaling_is_paused,
):
    mock_client = mock.MagicMock()
    # Create
    config_dict = {
        "max_instances": 3,
        "autoscaling": {"metrics_providers": [{"decision_policy": "bespoke"}]},
    }
    app = setup_app(config_dict, False)

    app.sync_horizontal_pod_autoscaler(kube_client=mock_client)

    assert (
        mock_client.autoscaling.replace_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert app.delete_horizontal_pod_autoscaler.call_count == 0
    assert (
        mock_client.autoscaling.create_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )


@mock.patch(
    "paasta_tools.kubernetes.application.controller_wrappers.autoscaling_is_paused",
    autospec=True,
)
def test_sync_horizontal_pod_autoscaler_update_hpa(mock_autoscaling_is_paused):
    mock_client = mock.MagicMock()
    # Update
    config_dict = {"max_instances": 3}
    app = setup_app(config_dict, True)

    mock_autoscaling_is_paused.return_value = False
    app.sync_horizontal_pod_autoscaler(kube_client=mock_client)
    assert (
        mock_client.autoscaling.create_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert app.delete_horizontal_pod_autoscaler.call_count == 0
    mock_client.autoscaling.replace_namespaced_horizontal_pod_autoscaler.assert_called_once_with(
        namespace="faasta",
        name="fake_name",
        body=app.soa_config.get_autoscaling_metric_spec(
            "fake_name",
            "cluster",
            mock_client,
            namespace="faasta",
        ),
        pretty=True,
    )


def test_job_wrapper_deep_delete():
    mock_client = mock.MagicMock()
    mock_job_spec = mock.MagicMock()
    mock_job_spec.metadata.name = "mock_job"
    mock_job_spec.metadata.namespace = "mock_namespace"
    job = JobWrapper(mock_job_spec)
    job.deep_delete(mock_client)
    mock_client.batches.delete_namespaced_job.assert_called_once_with(
        "mock_job",
        "mock_namespace",
        body=V1DeleteOptions(propagation_policy="Foreground"),
    )


def test_sync_service_creates_service_when_configured():
    """Test sync_service creates a Service when k8s_service is configured."""
    mock_client = mock.MagicMock()
    config_dict = {
        "k8s_service": {
            "headless": True,
            "port": 8888,
            "annotations": {
                "external-dns.alpha.kubernetes.io/hostname": "test1.example.com"
            },
        }
    }
    app = setup_app(config_dict, True)
    app.exists_service = mock.Mock(return_value=False)
    app.sync_service(kube_client=mock_client)
    mock_client.core.create_namespaced_service.assert_called_once()
    assert mock_client.core.replace_namespaced_service.call_count == 0
    assert mock_client.core.delete_namespaced_service.call_count == 0


def test_sync_service_updates_existing_service():
    """Test sync_service updates a Service when it already exists."""
    mock_client = mock.MagicMock()
    config_dict = {
        "k8s_service": {
            "headless": True,
            "port": 8889,
            "annotations": {
                "external-dns.alpha.kubernetes.io/hostname": "test2.example.com"
            },
        }
    }
    app = setup_app(config_dict, True)
    app.exists_service = mock.Mock(return_value=True)
    app.sync_service(kube_client=mock_client)
    mock_client.core.replace_namespaced_service.assert_called_once()
    assert mock_client.core.create_namespaced_service.call_count == 0
    assert mock_client.core.delete_namespaced_service.call_count == 0


def test_sync_service_deletes_when_config_removed():
    """Test sync_service deletes Service when k8s_service config is removed."""
    mock_client = mock.MagicMock()
    config_dict = {}
    app = setup_app(config_dict, True)
    app.exists_service = mock.Mock(return_value=True)
    app.sync_service(kube_client=mock_client)
    mock_client.core.delete_namespaced_service.assert_called_once()
    assert mock_client.core.create_namespaced_service.call_count == 0
    assert mock_client.core.replace_namespaced_service.call_count == 0


def test_sync_service_noop_when_not_configured_and_doesnt_exist():
    """Test sync_service does nothing when not configured and doesn't exist."""
    mock_client = mock.MagicMock()
    config_dict = {}
    app = setup_app(config_dict, True)
    app.exists_service = mock.Mock(return_value=False)
    app.sync_service(kube_client=mock_client)
    assert mock_client.core.create_namespaced_service.call_count == 0
    assert mock_client.core.replace_namespaced_service.call_count == 0
    assert mock_client.core.delete_namespaced_service.call_count == 0
