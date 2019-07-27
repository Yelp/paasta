import mock
from pytest import raises

from paasta_tools.list_kubernetes_service_instances import main
from paasta_tools.list_kubernetes_service_instances import parse_args


def test_parse_args():
    with mock.patch(
        "paasta_tools.list_kubernetes_service_instances.argparse.ArgumentParser",
        autospec=True,
    ) as mock_parser:
        assert parse_args() == mock_parser.return_value.parse_args()


def test_main():
    with mock.patch(
        "paasta_tools.list_kubernetes_service_instances.parse_args", autospec=True
    ) as mock_parse_args, mock.patch(
        "paasta_tools.list_kubernetes_service_instances.get_services_for_cluster",
        autospec=True,
        return_value=[("service1", "instance1"), ("service2", "instance1")],
    ) as mock_get_services_for_cluster, mock.patch(
        "paasta_tools.list_kubernetes_service_instances.paasta_print", autospec=True
    ) as mock_print:
        with raises(SystemExit) as e:
            main()
        assert e.value.code == 0
        mock_get_services_for_cluster.assert_called_with(
            cluster=mock_parse_args.return_value.cluster,
            instance_type="kubernetes",
            soa_dir=mock_parse_args.return_value.soa_dir,
        )
        mock_print.assert_called_with("service1.instance1\nservice2.instance1")
