import mock
import pytest

from paasta_tools.list_kubernetes_service_instances import main
from paasta_tools.list_kubernetes_service_instances import parse_args


def test_parse_args():
    with mock.patch(
        "paasta_tools.list_kubernetes_service_instances.argparse.ArgumentParser",
        autospec=True,
    ) as mock_parser:
        assert parse_args() == mock_parser.return_value.parse_args()


@mock.patch("paasta_tools.list_kubernetes_service_instances.parse_args", autospec=True)
@mock.patch(
    "paasta_tools.list_kubernetes_service_instances.get_services_for_cluster",
    autospec=True,
    return_value=[("service_1", "instance1"), ("service_2", "instance1")],
)
@mock.patch("builtins.print", autospec=True)
@pytest.mark.parametrize(
    "instance_type,sanitise,expected,shuffle,group_lines",
    [
        ("kubernetes", False, "service_1.instance1\nservice_2.instance1", False, None),
        ("kubernetes", True, "service--1-instance1\nservice--2-instance1", False, None),
        ("flink", True, "service--1-instance1\nservice--2-instance1", False, None),
        ("eks", False, "service_1.instance1\nservice_2.instance1", False, None),
        ("eks", True, "service--1-instance1\nservice--2-instance1", False, None),
    ],
)
def test_main(
    mock_print,
    mock_get_services,
    mock_parse_args,
    instance_type,
    sanitise,
    expected,
    shuffle,
    group_lines,
):
    mock_parse_args.return_value = mock.Mock(
        instance_type=instance_type,
        sanitise=sanitise,
        shuffle=shuffle,
        group_lines=group_lines,
    )

    with pytest.raises(SystemExit) as e:
        main()

    assert e.value.code == 0
    assert mock_get_services.call_args_list == [
        mock.call(
            cluster=mock_parse_args.return_value.cluster,
            instance_type=instance_type,
            soa_dir=mock_parse_args.return_value.soa_dir,
        )
    ]
    assert mock_print.call_args_list == [mock.call(expected)]
