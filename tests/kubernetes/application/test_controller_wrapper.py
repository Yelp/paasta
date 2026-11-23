from unittest import mock

import pytest
from kubernetes.client import V1DeleteOptions
from kubernetes.client.rest import ApiException

from paasta_tools.eks_tools import EksDeploymentConfig
from paasta_tools.kubernetes.application.controller_wrappers import Application
from paasta_tools.kubernetes.application.controller_wrappers import DeploymentWrapper
from paasta_tools.kubernetes.application.controller_wrappers import JobWrapper
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import SystemPaastaConfigDict


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


@pytest.fixture(autouse=True)
def mock_kubernetes_tools_load_system_paasta_config():
    config = SystemPaastaConfig(
        SystemPaastaConfigDict({}),
        "/mock/system/configs",
    )
    with mock.patch(
        "paasta_tools.kubernetes_tools.load_system_paasta_config",
        autospec=True,
        return_value=config,
    ) as m:
        yield m


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


VPA_API_KWARGS = {
    "group": "autoscaling.k8s.io",
    "version": "v1",
    "namespace": "faasta",
    "plural": "verticalpodautoscalers",
}


@pytest.fixture
def vpa_app():
    app = setup_app({"instances": 1}, exists_hpa=False)
    app.soa_config = EksDeploymentConfig(
        service="service",
        cluster="cluster",
        instance="instance",
        config_dict={"instances": 1},
        branch_dict=None,
    )
    with mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.load_system_paasta_config",
        autospec=True,
    ) as mock_load_system_paasta_config:

        def set_vpa_config(vpa_config):
            mock_load_system_paasta_config.return_value = SystemPaastaConfig(
                SystemPaastaConfigDict(vertical_pod_autoscaling=vpa_config),
                "/mock/system/configs",
            )

        yield app, set_vpa_config


def desired_vpa(app):
    return app.soa_config.get_vertical_pod_autoscaler(
        name="fake_name", namespace="faasta"
    )


def existing_vpa(managed, spec):
    labels = {"paasta.yelp.com/managed": "true"} if managed else {}
    return {"metadata": {"name": "fake_name", "labels": labels}, "spec": spec}


@pytest.mark.parametrize(
    "vpa_config",
    [{}, {"enabled": False, "instances": ["service.instance"]}],
)
def test_sync_vertical_pod_autoscaler_disabled(vpa_app, vpa_config):
    app, set_vpa_config = vpa_app
    set_vpa_config(vpa_config)
    mock_client = mock.MagicMock()

    app.sync_vertical_pod_autoscaler(kube_client=mock_client)

    assert mock_client.custom.method_calls == []


def test_sync_vertical_pod_autoscaler_skips_non_eks(vpa_app):
    app, set_vpa_config = vpa_app
    set_vpa_config({"enabled": True, "instances": ["service.instance"]})
    app.soa_config = KubernetesDeploymentConfig(
        service="service",
        cluster="cluster",
        instance="instance",
        config_dict={"instances": 1},
        branch_dict=None,
    )
    mock_client = mock.MagicMock()

    app.sync_vertical_pod_autoscaler(kube_client=mock_client)

    assert mock_client.custom.method_calls == []


def test_sync_vertical_pod_autoscaler_create(vpa_app):
    app, set_vpa_config = vpa_app
    set_vpa_config({"enabled": True, "instances": ["service.instance"]})
    mock_client = mock.MagicMock()
    mock_client.custom.get_namespaced_custom_object.side_effect = ApiException(
        status=404
    )

    app.sync_vertical_pod_autoscaler(kube_client=mock_client)

    mock_client.custom.create_namespaced_custom_object.assert_called_once_with(
        **VPA_API_KWARGS, body=desired_vpa(app)
    )
    assert mock_client.custom.patch_namespaced_custom_object.call_count == 0


def test_sync_vertical_pod_autoscaler_up_to_date(vpa_app):
    app, set_vpa_config = vpa_app
    set_vpa_config({"enabled": True, "instances": ["service.instance"]})
    mock_client = mock.MagicMock()
    mock_client.custom.get_namespaced_custom_object.return_value = existing_vpa(
        managed=True, spec=desired_vpa(app)["spec"]
    )

    app.sync_vertical_pod_autoscaler(kube_client=mock_client)

    assert mock_client.custom.create_namespaced_custom_object.call_count == 0
    assert mock_client.custom.patch_namespaced_custom_object.call_count == 0
    assert mock_client.custom.delete_namespaced_custom_object.call_count == 0


def test_sync_vertical_pod_autoscaler_patches_changed_spec(vpa_app):
    app, set_vpa_config = vpa_app
    set_vpa_config({"enabled": True, "instances": ["service.instance"]})
    mock_client = mock.MagicMock()
    mock_client.custom.get_namespaced_custom_object.return_value = existing_vpa(
        managed=True, spec={"updatePolicy": {"updateMode": "Auto"}}
    )

    app.sync_vertical_pod_autoscaler(kube_client=mock_client)

    mock_client.custom.patch_namespaced_custom_object.assert_called_once_with(
        **VPA_API_KWARGS,
        name="fake_name",
        body={"spec": desired_vpa(app)["spec"]},
    )
    assert mock_client.custom.create_namespaced_custom_object.call_count == 0


@pytest.mark.parametrize(
    "vpa_config",
    [{"enabled": True}, {"enabled": True, "instances": ["other_service.main"]}],
)
def test_sync_vertical_pod_autoscaler_unlisted_makes_no_api_calls(vpa_app, vpa_config):
    app, set_vpa_config = vpa_app
    set_vpa_config(vpa_config)
    mock_client = mock.MagicMock()

    app.sync_vertical_pod_autoscaler(kube_client=mock_client)

    assert mock_client.custom.method_calls == []


def test_sync_vertical_pod_autoscaler_leaves_unmanaged_vpa_alone(vpa_app):
    app, set_vpa_config = vpa_app
    set_vpa_config({"enabled": True, "instances": ["service.instance"]})
    mock_client = mock.MagicMock()
    mock_client.custom.get_namespaced_custom_object.return_value = existing_vpa(
        managed=False, spec={"updatePolicy": {"updateMode": "Off"}}
    )

    app.sync_vertical_pod_autoscaler(kube_client=mock_client)

    assert mock_client.custom.create_namespaced_custom_object.call_count == 0
    assert mock_client.custom.patch_namespaced_custom_object.call_count == 0
    assert mock_client.custom.delete_namespaced_custom_object.call_count == 0


def test_sync_vertical_pod_autoscaler_reraises_api_errors(vpa_app):
    app, set_vpa_config = vpa_app
    set_vpa_config({"enabled": True, "instances": ["service.instance"]})
    mock_client = mock.MagicMock()
    mock_client.custom.get_namespaced_custom_object.side_effect = ApiException(
        status=403
    )

    with pytest.raises(ApiException):
        app.sync_vertical_pod_autoscaler(kube_client=mock_client)
    assert mock_client.custom.create_namespaced_custom_object.call_count == 0


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
