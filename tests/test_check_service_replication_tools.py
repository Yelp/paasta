import mock

from paasta_tools import check_services_replication_tools
from paasta_tools.utils import SystemPaastaConfig


def test_main_kubernetes():
    with mock.patch(
        "paasta_tools.check_services_replication_tools.check_services_replication",
        autospec=True,
    ) as mock_check_services_replication, mock.patch(
        "paasta_tools.check_services_replication_tools.parse_args", autospec=True
    ), mock.patch(
        "paasta_tools.check_services_replication_tools.get_kubernetes_pods_and_nodes",
        autospec=True,
        return_value=([mock.Mock()], [mock.Mock()]),
    ):
        check_services_replication_tools.main(
            instance_type_class=None, check_service_replication=None, namespace="baz",
        )
        assert mock_check_services_replication.called


def test_main_mesos():
    with mock.patch(
        "paasta_tools.check_services_replication_tools.check_services_replication",
        autospec=True,
    ) as mock_check_services_replication, mock.patch(
        "paasta_tools.check_services_replication_tools.parse_args", autospec=True
    ), mock.patch(
        "paasta_tools.check_services_replication_tools.get_mesos_tasks_and_slaves",
        autospec=True,
        return_value=([mock.Mock()], [mock.Mock()]),
    ):
        check_services_replication_tools.main(
            instance_type_class=None,
            check_service_replication=None,
            namespace=None,
            mesos=True,
        )
        assert mock_check_services_replication.called


def test_check_services_replication():
    soa_dir = "anw"
    instance_config = mock.Mock()
    instance_config.get_docker_image.return_value = True
    with mock.patch(
        "paasta_tools.check_services_replication_tools.list_services",
        autospec=True,
        return_value=["a"],
    ), mock.patch(
        "paasta_tools.check_kubernetes_services_replication.check_kubernetes_pod_replication",
        autospec=True,
    ) as mock_check_service_replication, mock.patch(
        "paasta_tools.check_services_replication_tools.PaastaServiceConfigLoader",
        autospec=True,
    ) as mock_paasta_service_config_loader, mock.patch(
        "paasta_tools.check_services_replication_tools.KubeClient", autospec=True
    ) as mock_kube_client:
        mock_kube_client.return_value = mock.Mock()
        mock_paasta_service_config_loader.return_value.instance_configs.return_value = [
            instance_config
        ]
        mock_client = mock.Mock()
        mock_client.list_tasks.return_value = []
        mock_system_paasta_config = mock.Mock(spec=SystemPaastaConfig)
        mock_replication_checker = mock.Mock()
        mock_pods = [mock.Mock(), mock.Mock()]

        check_services_replication_tools.check_services_replication(
            soa_dir=soa_dir,
            system_paasta_config=mock_system_paasta_config,
            service_instances=[],
            instance_type_class=None,
            check_service_replication=mock_check_service_replication,
            replication_checker=mock_replication_checker,
            all_tasks_or_pods=mock_pods,
        )
        mock_paasta_service_config_loader.assert_called_once_with(
            service="a", soa_dir=soa_dir
        )
        instance_config.get_docker_image.assert_called_once_with()
        mock_check_service_replication.assert_called_once_with(
            instance_config=instance_config,
            all_tasks_or_pods=mock_pods,
            smartstack_replication_checker=mock_replication_checker,
        )
