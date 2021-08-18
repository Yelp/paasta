import mock

from paasta_tools.setup_istio_mesh import setup_kube_services


def test_setup_kube_service():
    with mock.patch(
        "paasta_tools.setup_kubernetes_services.list_kubernetes_services", autospec=True
    ) as mock_list_services, mock.patch(
        "paasta_tools.setup_kubernetes_services.log", autospec=True
    ):
        mock_client = mock.Mock()

        mock_list_services.return_value = mock.Mock(
            items=[mock.Mock(metadata=mock.Mock(name="kurupt.f_m"))]
        )

        mock_service_instances = ["kurupt.f_m"]

        setup_kube_services(
            kube_client=mock_client, service_instances=mock_service_instances,
        )

        assert mock_client.core.create_namespaced_service.call_count == 1
        assert (
            mock_client.core.create_namespaced_service.call_args[0][1].metadata.name
            == "kurupt.f_m"
        )
        assert mock_client.core.create_namespaced_service.call_args[0][
            1
        ].spec.selector == {f"registrations.paasta.yelp.com/kurupt.f_m": True}


def test_setup_kube_service_invalid_job_name():
    with mock.patch(
        "paasta_tools.setup_kubernetes_services.list_kubernetes_services", autospec=True
    ) as mock_list_services, mock.patch(
        "paasta_tools.setup_kubernetes_services.log", autospec=True
    ):
        mock_client = mock.Mock()

        mock_list_services.return_value = mock.Mock(
            items=[mock.Mock(metadata=mock.Mock(name="kurupt.f_m"))]
        )

        mock_service_instances = ["kuruptf_m"]

        setup_kube_services(
            kube_client=mock_client, service_instances=mock_service_instances,
        )

        assert mock_client.core.create_namespaced_service.call_count == 0


def test_setup_kube_services_rate_limit():
    with mock.patch(
        "paasta_tools.setup_kubernetes_services.list_kubernetes_services", autospec=True
    ), mock.patch(
        "paasta_tools.setup_kubernetes_services.log", autospec=True
    ) as mock_log_obj:
        mock_client = mock.Mock()
        mock_service_instances = ["kurupt.fm", "kurupt.garage", "kurupt.radio"]

        # Rate limit: 2 calls allowed
        setup_kube_services(
            kube_client=mock_client,
            service_instances=mock_service_instances,
            rate_limit=2,
        )

        assert mock_client.core.create_namespaced_service.call_count == 2

        mock_log_obj.info.assert_any_call(
            "Not doing any further updates as we reached the limit (2)"
        )

        # No rate limit
        mock_client.reset_mock()

        setup_kube_services(
            kube_client=mock_client,
            service_instances=mock_service_instances,
            rate_limit=0,
        )

        assert mock_client.core.create_namespaced_service.call_count == 3
