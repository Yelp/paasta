import mock
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes.application.controller_wrappers import Application
from paasta_tools.kubernetes.application.controller_wrappers import DeploymentWrapper


def test_ensure_pod_disruption_budget_create():
    with mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.pod_disruption_budget_for_service_instance",
        autospec=True,
    ) as mock_pdr_for_service_instance:
        mock_req_pdr = mock.Mock()
        mock_req_pdr.spec.max_unavailable = 10
        mock_pdr_for_service_instance.return_value = mock_req_pdr

        mock_client = mock.MagicMock()

        mock_client.policy.read_namespaced_pod_disruption_budget.side_effect = ApiException(
            status=404
        )

        app = mock.MagicMock()
        app.soa_config.get_bounce_margin_factor.return_value = 0.1
        app.kube_deployment.service.return_value = "fake_service"
        app.kube_deployment.instance.return_value = "fake_instance"
        Application.ensure_pod_disruption_budget(self=app, kube_client=mock_client)
        mock_client.policy.create_namespaced_pod_disruption_budget.assert_called_once_with(
            namespace="paasta", body=mock_req_pdr
        )


def test_ensure_pod_disruption_budget_replaces_outdated():
    with mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.pod_disruption_budget_for_service_instance",
        autospec=True,
    ) as mock_pdr_for_service_instance:
        mock_req_pdr = mock.Mock()
        mock_req_pdr.spec.max_unavailable = 10
        mock_pdr_for_service_instance.return_value = mock_req_pdr

        mock_client = mock.MagicMock()

        mock_pdr = mock.Mock()
        mock_pdr.spec.max_unavailable = 5

        mock_client.policy.read_namespaced_pod_disruption_budget.return_value = mock_pdr

        app = mock.MagicMock()
        app.soa_config.get_bounce_margin_factor.return_value = 0.1
        app.kube_deployment.service.return_value = "fake_service"
        app.kube_deployment.instance.return_value = "fake_instance"
        Application.ensure_pod_disruption_budget(self=app, kube_client=mock_client)

        mock_client.policy.patch_namespaced_pod_disruption_budget.assert_called_once_with(
            name=mock_req_pdr.metadata.name,
            namespace=mock_req_pdr.metadata.namespace,
            body=mock_req_pdr,
        )


def test_sync_horizontal_pod_autoscaler():
    mock_client = mock.MagicMock()
    app = mock.MagicMock()
    app.item.metadata.name = "fake_name"
    app.item.metadata.namespace = "faasta"

    # helper Functions for mocking
    def setup_app(app, config_dict, exists_hpa):
        app.reset_mock()
        mock_client.reset_mock()
        app.get_soa_config.return_value = config_dict
        app.exists_hpa.return_value = exists_hpa

    # Do nothing
    config_dict = {"instances": 1}
    setup_app(app, config_dict, False)
    DeploymentWrapper.sync_horizontal_pod_autoscaler(self=app, kube_client=mock_client)
    assert (
        mock_client.autoscaling.create_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert (
        mock_client.autoscaling.patch_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert app.delete_horizontal_pod_autoscaler.call_count == 0

    # old HPA got removed so delete
    config_dict = {"instances": 1}
    setup_app(app, config_dict, True)
    DeploymentWrapper.sync_horizontal_pod_autoscaler(self=app, kube_client=mock_client)
    assert (
        mock_client.autoscaling.create_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert (
        mock_client.autoscaling.patch_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert app.delete_horizontal_pod_autoscaler.call_count == 1

    # Create
    config_dict = {}
    setup_app(app, config_dict, False)
    DeploymentWrapper.sync_horizontal_pod_autoscaler(self=app, kube_client=mock_client)
    assert (
        mock_client.autoscaling.patch_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert app.delete_horizontal_pod_autoscaler.call_count == 0
    mock_client.autoscaling.create_namespaced_horizontal_pod_autoscaler.assert_called_once_with(
        namespace="faasta",
        body=app.soa_config.get_autoscaling_metric_spec.return_value,
        pretty=True,
    )

    # Update
    config_dict = {}
    setup_app(app, config_dict, True)
    DeploymentWrapper.sync_horizontal_pod_autoscaler(self=app, kube_client=mock_client)
    assert (
        mock_client.autoscaling.create_namespaced_horizontal_pod_autoscaler.call_count
        == 0
    )
    assert app.delete_horizontal_pod_autoscaler.call_count == 0
    mock_client.autoscaling.patch_namespaced_horizontal_pod_autoscaler.assert_called_once_with(
        namespace="faasta",
        name="fake_name",
        body=app.soa_config.get_autoscaling_metric_spec.return_value,
        pretty=True,
    )
