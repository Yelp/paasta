import mock
from kubernetes.client import V1DeleteOptions
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
    with mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.DeploymentWrapper.exists_hpa",
        autospec=True,
    ) as mock_exists_hpa, mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.DeploymentWrapper.delete_horizontal_pod_autoscaler",
        autospec=True,
    ) as mock_delete_hpa, mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.Application.get_soa_config",
        autospec=True,
    ) as mock_soa_config, mock.patch(
        "paasta_tools.kubernetes.application.controller_wrappers.Application.get_soa_config",
        autospec=True,
    ):
        mock_client = mock.MagicMock()
        app = mock.MagicMock()
        app.kube_deployment.service.return_value = "fake_service"
        app.kube_deployment.instance.return_value = "fake_instance"

        # Do nothing
        config_dict = {"instances": 1}
        mock_soa_config.return_value = config_dict
        mock_exists_hpa.return_value = False
        DeploymentWrapper.ensure_pod_disruption_budget(
            self=app, kube_client=mock_client
        )
        assert mock_delete_hpa.call_count == 1

        # old HPA got removed so delete
        config_dict = {"instances": 1}
        mock_soa_config.return_value = config_dict
        mock_exists_hpa.return_value = True
        DeploymentWrapper.ensure_pod_disruption_budget(
            self=app, kube_client=mock_client
        )
        assert mock_delete_hpa.call_count == 1

        # Called with mesos HPA
