import mock
from kubernetes.client import V1DeleteOptions
from kubernetes.client.rest import ApiException

from paasta_tools.kubernetes.application.controller_wrappers import Application


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
        app.item.service.return_value = "fake_service"
        app.item.instance.return_value = "fake_instance"
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
        app.item.service.return_value = "fake_service"
        app.item.instance.return_value = "fake_instance"
        Application.ensure_pod_disruption_budget(self=app, kube_client=mock_client)

        mock_client.policy.delete_namespaced_pod_disruption_budget.assert_called_once_with(
            name=mock_req_pdr.metadata.name,
            namespace=mock_req_pdr.metadata.namespace,
            body=V1DeleteOptions(),
        )
        mock_create_pdr.assert_called_once_with(
            kube_client=mock_client, pod_disruption_budget=mock_req_pdr
        )
