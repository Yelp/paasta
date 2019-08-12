import mock
from kubernetes.client import V1DeleteOptions
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V2beta1CrossVersionObjectReference
from kubernetes.client import V2beta1HorizontalPodAutoscaler
from kubernetes.client import V2beta1HorizontalPodAutoscalerSpec
from kubernetes.client import V2beta1MetricSpec
from kubernetes.client import V2beta1PodsMetricSource
from kubernetes.client import V2beta1ResourceMetricSource
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes.application.controller_wrappers import Application
from paasta_tools.kubernetes.application.controller_wrappers import DeploymentWrapper


def test_ensure_pod_disruption_budget_create():
    with mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.pod_disruption_budget_for_service_instance",
        autospec=True,
    ) as mock_pdr_for_service_instance, mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.create_pod_disruption_budget",
        autospec=True,
    ) as mock_create_pdr, mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.max_unavailable",
        autospec=True,
        return_value=0,
    ):
        mock_req_pdr = mock.Mock()
        mock_req_pdr.spec.min_available = 10
        mock_pdr_for_service_instance.return_value = mock_req_pdr

        mock_client = mock.Mock()

        mock_pdr = mock.Mock()
        mock_pdr.spec.min_available = 10

        mock_client.read_namespaced_pod_disruption_budget.side_effect = ApiException(
            status=404
        )

        app = mock.MagicMock()
        app.soa_config.get_desired_instances.return_value = 10
        app.kube_deployment.service.return_value = "fake_service"
        app.kube_deployment.instance.return_value = "fake_instance"
        Application.ensure_pod_disruption_budget(self=app, kube_client=mock_client)
        mock_create_pdr.assert_called_once_with(
            kube_client=mock_client, pod_disruption_budget=mock_req_pdr
        )


def test_ensure_pod_disruption_budget_replaces_outdated():
    with mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.pod_disruption_budget_for_service_instance",
        autospec=True,
    ) as mock_pdr_for_service_instance, mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.create_pod_disruption_budget",
        autospec=True,
    ) as mock_create_pdr, mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.max_unavailable",
        autospec=True,
        return_value=0,
    ):
        mock_req_pdr = mock.Mock()
        mock_req_pdr.spec.min_available = 10
        mock_pdr_for_service_instance.return_value = mock_req_pdr

        mock_client = mock.Mock()

        mock_pdr = mock.Mock()
        mock_pdr.spec.min_available = 10

        mock_client.read_namespaced_pod_disruption_budget.return_value = mock_pdr

        app = mock.MagicMock()
        app.soa_config.get_desired_instances.return_value = 10
        app.kube_deployment.service.return_value = "fake_service"
        app.kube_deployment.instance.return_value = "fake_instance"
        Application.ensure_pod_disruption_budget(self=app, kube_client=mock_client)

        mock_client.policy.delete_namespaced_pod_disruption_budget.assert_called_once_with(
            name=mock_req_pdr.metadata.name,
            namespace=mock_req_pdr.metadata.namespace,
            body=V1DeleteOptions(),
        )
        mock_create_pdr.assert_called_once_with(
            kube_client=mock_client, pod_disruption_budget=mock_req_pdr
        )


def test_sync_horizontal_pod_autoscaler():
    mock_client = mock.MagicMock()
    app = mock.MagicMock()
    app.item.metadata.name = "fake_name"
    app.item.metadata.namespace = "faasta"

    # Do nothing
    app.reset_mock()
    mock_client.reset_mock()
    config_dict = {"instances": 1}
    app.get_soa_config.return_value = config_dict
    app.exists_hpa.return_value = False
    DeploymentWrapper.sync_horizontal_pod_autoscaler(self=app, kube_client=mock_client)
    assert (
        mock_client.autoscaling.create_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert (
        mock_client.autoscaling.update_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert app.delete_horizontal_pod_autoscaler.call_count == 0

    # old HPA got removed so delete
    app.reset_mock()
    mock_client.reset_mock()
    config_dict = {"instances": 1}
    app.get_soa_config.return_value = config_dict
    app.exists_hpa.return_value = True
    DeploymentWrapper.sync_horizontal_pod_autoscaler(self=app, kube_client=mock_client)
    assert (
        mock_client.autoscaling.create_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert (
        mock_client.autoscaling.update_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert app.delete_horizontal_pod_autoscaler.call_count == 1

    # Create new HPA with cpu
    app.reset_mock()
    mock_client.reset_mock()
    config_dict = {
        "min_instances": 1,
        "max_instances": 3,
        "autoscaling": {"metrics_provider": "mesos_cpu", "setpoint": "0.5"},
    }

    app.get_soa_config.return_value = config_dict
    app.exists_hpa.return_value = False
    DeploymentWrapper.sync_horizontal_pod_autoscaler(self=app, kube_client=mock_client)
    assert (
        mock_client.autoscaling.update_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert app.delete_horizontal_pod_autoscaler.call_count == 0
    mock_client.autoscaling.create_namespaced_horizontal_pod_autoscaler.assert_called_once_with(
        namespace="faasta",
        body=V2beta1HorizontalPodAutoscaler(
            kind="HorizontalPodAutoscaler",
            metadata=V1ObjectMeta(name="fake_name", namespace="faasta"),
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
                    kind="Deployment", name="fake_name"
                ),
            ),
        ),
        pretty=True,
    )

    # Update new HPA with cpu
    app.reset_mock()
    mock_client.reset_mock()
    config_dict = {
        "min_instances": 1,
        "max_instances": 3,
        "autoscaling": {"metrics_provider": "mesos_cpu", "setpoint": "0.5"},
    }

    app.get_soa_config.return_value = config_dict
    app.exists_hpa.return_value = True
    DeploymentWrapper.sync_horizontal_pod_autoscaler(self=app, kube_client=mock_client)
    assert (
        mock_client.autoscaling.update_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert app.delete_horizontal_pod_autoscaler.call_count == 0
    mock_client.autoscaling.patch_namespaced_horizontal_pod_autoscaler.assert_called_once_with(
        namespace="faasta",
        body=V2beta1HorizontalPodAutoscaler(
            kind="HorizontalPodAutoscaler",
            metadata=V1ObjectMeta(name="fake_name", namespace="faasta"),
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
                    kind="Deployment", name="fake_name"
                ),
            ),
        ),
        pretty=True,
        name="fake_name",
    )

    # update http
    app.reset_mock()
    mock_client.reset_mock()
    config_dict = {
        "min_instances": 1,
        "max_instances": 3,
        "autoscaling": {"metrics_provider": "http", "setpoint": "0.5"},
    }

    app.get_soa_config.return_value = config_dict
    app.exists_hpa.return_value = True
    DeploymentWrapper.sync_horizontal_pod_autoscaler(self=app, kube_client=mock_client)
    assert (
        mock_client.autoscaling.update_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert app.delete_horizontal_pod_autoscaler.call_count == 0
    mock_client.autoscaling.patch_namespaced_horizontal_pod_autoscaler.assert_called_once_with(
        namespace="faasta",
        body=V2beta1HorizontalPodAutoscaler(
            kind="HorizontalPodAutoscaler",
            metadata=V1ObjectMeta(name="fake_name", namespace="faasta"),
            spec=V2beta1HorizontalPodAutoscalerSpec(
                max_replicas=3,
                min_replicas=1,
                metrics=[
                    V2beta1MetricSpec(
                        type="Pods",
                        pods=V2beta1PodsMetricSource(
                            metric_name="http", target_average_value=50.0
                        ),
                    )
                ],
                scale_target_ref=V2beta1CrossVersionObjectReference(
                    kind="Deployment", name="fake_name"
                ),
            ),
        ),
        pretty=True,
        name="fake_name",
    )

    # update uwsgi
    app.reset_mock()
    mock_client.reset_mock()
    config_dict = {
        "min_instances": 1,
        "max_instances": 3,
        "autoscaling": {"metrics_provider": "uwsgi", "setpoint": "0.5"},
    }

    app.get_soa_config.return_value = config_dict
    app.exists_hpa.return_value = True
    DeploymentWrapper.sync_horizontal_pod_autoscaler(self=app, kube_client=mock_client)
    assert (
        mock_client.autoscaling.update_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert app.delete_horizontal_pod_autoscaler.call_count == 0
    mock_client.autoscaling.patch_namespaced_horizontal_pod_autoscaler.assert_called_once_with(
        namespace="faasta",
        body=V2beta1HorizontalPodAutoscaler(
            kind="HorizontalPodAutoscaler",
            metadata=V1ObjectMeta(name="fake_name", namespace="faasta"),
            spec=V2beta1HorizontalPodAutoscalerSpec(
                max_replicas=3,
                min_replicas=1,
                metrics=[
                    V2beta1MetricSpec(
                        type="Pods",
                        pods=V2beta1PodsMetricSource(
                            metric_name="uwsgi", target_average_value=50.0
                        ),
                    )
                ],
                scale_target_ref=V2beta1CrossVersionObjectReference(
                    kind="Deployment", name="fake_name"
                ),
            ),
        ),
        pretty=True,
        name="fake_name",
    )
