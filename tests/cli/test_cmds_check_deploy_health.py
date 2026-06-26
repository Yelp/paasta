from unittest import mock
from unittest.mock import patch

from paasta_tools.cli.cmds import check_deploy_health
from paasta_tools.cli.cmds.mark_for_deployment import NoSuchCluster
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.utils import DeploymentVersion


@patch(
    "paasta_tools.cli.cmds.check_deploy_health.check_if_instance_is_done",
    autospec=True,
)
@patch(
    "paasta_tools.cli.cmds.check_deploy_health.get_instance_configs_for_service_in_deploy_group_all_clusters",
    autospec=True,
)
@patch(
    "paasta_tools.cli.cmds.check_deploy_health.get_currently_deployed_version",
    autospec=True,
)
@patch(
    "paasta_tools.cli.cmds.check_deploy_health.validate_service_name",
    autospec=True,
)
def test_check_deploy_health_all_healthy(
    mock_validate,
    mock_get_version,
    mock_get_instance_configs,
    mock_check_done,
):
    mock_get_version.return_value = DeploymentVersion(sha="abc123", image_version="v1")
    mock_instance_config = mock.Mock(spec=LongRunningServiceConfig)
    mock_instance_config.get_instance.return_value = "main"
    mock_get_instance_configs.return_value = {"cluster1": [mock_instance_config]}
    mock_check_done.return_value = True

    args = mock.Mock()
    args.service = "test_service"
    args.deploy_group = "prod.main"
    args.soa_dir = "/fake/soa"

    ret = check_deploy_health.paasta_check_deploy_health(args)
    assert ret == 0
    mock_check_done.assert_called_once_with(
        service="test_service",
        instance="main",
        cluster="cluster1",
        version=DeploymentVersion(sha="abc123", image_version="v1"),
        instance_config=mock_instance_config,
    )


@patch(
    "paasta_tools.cli.cmds.check_deploy_health.check_if_instance_is_done",
    autospec=True,
)
@patch(
    "paasta_tools.cli.cmds.check_deploy_health.get_instance_configs_for_service_in_deploy_group_all_clusters",
    autospec=True,
)
@patch(
    "paasta_tools.cli.cmds.check_deploy_health.get_currently_deployed_version",
    autospec=True,
)
@patch(
    "paasta_tools.cli.cmds.check_deploy_health.validate_service_name",
    autospec=True,
)
def test_check_deploy_health_unhealthy(
    mock_validate,
    mock_get_version,
    mock_get_instance_configs,
    mock_check_done,
):
    mock_get_version.return_value = DeploymentVersion(sha="abc123", image_version="v1")
    mock_config_1 = mock.Mock(spec=LongRunningServiceConfig)
    mock_config_1.get_instance.return_value = "main"
    mock_config_2 = mock.Mock(spec=LongRunningServiceConfig)
    mock_config_2.get_instance.return_value = "canary"
    mock_get_instance_configs.return_value = {
        "cluster1": [mock_config_1, mock_config_2]
    }
    mock_check_done.side_effect = [True, False]

    args = mock.Mock()
    args.service = "test_service"
    args.deploy_group = "prod.main"
    args.soa_dir = "/fake/soa"

    ret = check_deploy_health.paasta_check_deploy_health(args)
    assert ret == 1


@patch(
    "paasta_tools.cli.cmds.check_deploy_health.get_currently_deployed_version",
    autospec=True,
)
@patch(
    "paasta_tools.cli.cmds.check_deploy_health.validate_service_name",
    autospec=True,
)
def test_check_deploy_health_no_version_deployed(
    mock_validate,
    mock_get_version,
):
    mock_get_version.return_value = None

    args = mock.Mock()
    args.service = "test_service"
    args.deploy_group = "prod.main"
    args.soa_dir = "/fake/soa"

    ret = check_deploy_health.paasta_check_deploy_health(args)
    assert ret == 2


@patch(
    "paasta_tools.cli.cmds.check_deploy_health.get_instance_configs_for_service_in_deploy_group_all_clusters",
    autospec=True,
)
@patch(
    "paasta_tools.cli.cmds.check_deploy_health.get_currently_deployed_version",
    autospec=True,
)
@patch(
    "paasta_tools.cli.cmds.check_deploy_health.validate_service_name",
    autospec=True,
)
def test_check_deploy_health_no_instance_configs(
    mock_validate,
    mock_get_version,
    mock_get_instance_configs,
):
    mock_get_version.return_value = DeploymentVersion(sha="abc123", image_version=None)
    mock_get_instance_configs.return_value = {}

    args = mock.Mock()
    args.service = "test_service"
    args.deploy_group = "prod.main"
    args.soa_dir = "/fake/soa"

    ret = check_deploy_health.paasta_check_deploy_health(args)
    assert ret == 2


@patch(
    "paasta_tools.cli.cmds.check_deploy_health.validate_service_name",
    autospec=True,
)
def test_check_deploy_health_invalid_service(mock_validate):
    mock_validate.side_effect = Exception("Service not found")

    args = mock.Mock()
    args.service = "nonexistent"
    args.deploy_group = "prod.main"
    args.soa_dir = "/fake/soa"

    ret = check_deploy_health.paasta_check_deploy_health(args)
    assert ret == 2


@patch(
    "paasta_tools.cli.cmds.check_deploy_health.get_instance_configs_for_service_in_deploy_group_all_clusters",
    autospec=True,
)
@patch(
    "paasta_tools.cli.cmds.check_deploy_health.get_currently_deployed_version",
    autospec=True,
)
@patch(
    "paasta_tools.cli.cmds.check_deploy_health.validate_service_name",
    autospec=True,
)
def test_check_deploy_health_no_such_cluster(
    mock_validate,
    mock_get_version,
    mock_get_instance_configs,
):
    mock_get_version.return_value = DeploymentVersion(sha="abc123", image_version="v1")
    mock_get_instance_configs.side_effect = NoSuchCluster

    args = mock.Mock()
    args.service = "test_service"
    args.deploy_group = "prod.main"
    args.soa_dir = "/fake/soa"

    ret = check_deploy_health.paasta_check_deploy_health(args)
    assert ret == 2


@patch(
    "paasta_tools.cli.cmds.check_deploy_health.get_instance_configs_for_service_in_deploy_group_all_clusters",
    autospec=True,
)
@patch(
    "paasta_tools.cli.cmds.check_deploy_health.get_currently_deployed_version",
    autospec=True,
)
@patch(
    "paasta_tools.cli.cmds.check_deploy_health.validate_service_name",
    autospec=True,
)
def test_check_deploy_health_empty_instance_lists(
    mock_validate,
    mock_get_version,
    mock_get_instance_configs,
):
    """Dict with clusters but no matching instances should return error, not HEALTHY."""
    mock_get_version.return_value = DeploymentVersion(sha="abc123", image_version="v1")
    mock_get_instance_configs.return_value = {"cluster1": [], "cluster2": []}

    args = mock.Mock()
    args.service = "test_service"
    args.deploy_group = "prod.main"
    args.soa_dir = "/fake/soa"

    ret = check_deploy_health.paasta_check_deploy_health(args)
    assert ret == 2
