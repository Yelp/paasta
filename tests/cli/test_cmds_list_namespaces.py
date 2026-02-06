# tests/cli/cmds/test_list_namespaces.py
from unittest.mock import MagicMock
from unittest.mock import patch

from paasta_tools.cli.cmds.list_namespaces import paasta_list_namespaces
from paasta_tools.spark_tools import SPARK_EXECUTOR_NAMESPACE


def test_list_namespaces_no_instances(capfd):
    mock_args = MagicMock(
        service="fake_service",
        instance=None,
        cluster=None,
        soa_dir="/fake/soa/dir",
    )
    with (
        patch(
            "paasta_tools.cli.cmds.list_namespaces.get_instance_configs_for_service",
            return_value=[],
            autospec=True,
        ),
        patch(
            "paasta_tools.cli.cmds.list_namespaces.validate_service_name",
            autospec=True,
        ),
    ):
        assert paasta_list_namespaces(mock_args) == 0
        stdout, _ = capfd.readouterr()
        assert stdout.strip() == "[]"


def create_mock_instance_config(instance_type, namespace):
    """
    Creates a mock InstanceConfig with specified instance_type and namespace.

    :param instance_type: The type of the instance (e.g., "kubernetes", "paasta-native").
    :param namespace: The namespace associated with the instance.
    :return: A mock InstanceConfig object.
    """
    mock_instance_config = MagicMock()
    mock_instance_config.get_instance_type.return_value = instance_type
    mock_instance_config.get_namespace.return_value = namespace
    return mock_instance_config


def test_list_namespaces_with_instances_dupe_ns(capfd):
    mock_args = MagicMock(
        service="fake_service",
        instance=None,
        cluster=None,
        soa_dir="/fake/soa/dir",
    )

    mock_instance_configs = [
        create_mock_instance_config("kubernetes", "k8s_namespace"),
        create_mock_instance_config("kubernetes", "k8s_namespace"),
    ]

    with (
        patch(
            "paasta_tools.cli.cmds.list_namespaces.get_instance_configs_for_service",
            return_value=mock_instance_configs,
            autospec=True,
        ),
        patch(
            "paasta_tools.cli.cmds.list_namespaces.validate_service_name",
            autospec=True,
        ),
    ):
        assert paasta_list_namespaces(mock_args) == 0
        stdout, _ = capfd.readouterr()
        assert stdout.strip() == "['k8s_namespace']"


def test_list_namespaces_tron(capfd):
    mock_args = MagicMock(
        service="fake_service",
        instance=None,
        cluster=None,
        soa_dir="/fake/soa/dir",
    )
    mock_tron_instance = create_mock_instance_config("tron", "tron")
    mock_tron_instance.get_executor.return_value = "paasta"

    with (
        patch(
            "paasta_tools.cli.cmds.list_namespaces.get_instance_configs_for_service",
            return_value=[mock_tron_instance],
            autospec=True,
        ),
        patch(
            "paasta_tools.cli.cmds.list_namespaces.validate_service_name",
            autospec=True,
        ),
    ):
        assert paasta_list_namespaces(mock_args) == 0
        stdout, _ = capfd.readouterr()
        assert "['tron']"


def test_list_namespaces_spark(capfd):
    mock_args = MagicMock(
        service="fake_service",
        instance=None,
        cluster=None,
        soa_dir="/fake/soa/dir",
    )
    mock_spark_instance = create_mock_instance_config("tron", "tron")
    mock_spark_instance.get_executor.return_value = "spark"

    mock_instance_configs = [
        create_mock_instance_config("kubernetes", "k8s_namespace"),
        mock_spark_instance,
    ]

    with (
        patch(
            "paasta_tools.cli.cmds.list_namespaces.get_instance_configs_for_service",
            return_value=mock_instance_configs,
            autospec=True,
        ),
        patch(
            "paasta_tools.cli.cmds.list_namespaces.validate_service_name",
            autospec=True,
        ),
    ):
        assert paasta_list_namespaces(mock_args) == 0
        stdout, _ = capfd.readouterr()
        assert f"'{SPARK_EXECUTOR_NAMESPACE}'" in stdout
        assert "'tron'" in stdout
        assert "'k8s_namespace'" in stdout


def test_list_namespaces_skips_non_k8s_instances(capfd):
    mock_args = MagicMock(
        service="fake_service",
        instance=None,
        cluster=None,
        soa_dir="/fake/soa/dir",
    )

    mock_k8s_instance_config = create_mock_instance_config("eks", "k8s_namespace")
    mock_adhoc_instance_config = create_mock_instance_config("adhoc", None)

    with (
        patch(
            "paasta_tools.cli.cmds.list_namespaces.get_instance_configs_for_service",
            return_value=[mock_k8s_instance_config, mock_adhoc_instance_config],
            autospec=True,
        ),
        patch(
            "paasta_tools.cli.cmds.list_namespaces.validate_service_name",
            autospec=True,
        ),
    ):
        assert paasta_list_namespaces(mock_args) == 0
        stdout, _ = capfd.readouterr()
        assert stdout.strip() == "['k8s_namespace']"
