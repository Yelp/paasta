import mock

from paasta_tools.nrtsearchserviceeks_tools import (
    load_nrtsearchserviceeks_instance_config,
)


def test_load_nrtsearchserviceeks_instance_config():
    with mock.patch(
        "paasta_tools.nrtsearchserviceeks_tools.load_v2_deployments_json", autospec=True
    ) as mock_load_v2_deployments_json, mock.patch(
        "paasta_tools.nrtsearchserviceeks_tools.load_service_instance_config",
        autospec=True,
    ), mock.patch(
        "paasta_tools.nrtsearchserviceeks_tools.NrtsearchServiceEksDeploymentConfig",
        autospec=True,
    ) as mock_nrtsearchserviceeks_deployment_config:
        mock_config = {
            "port": None,
            "monitoring": {},
            "deploy": {},
            "data": {},
            "smartstack": {},
            "dependencies": {},
        }
        nrtsearchserviceeks_deployment_config = (
            load_nrtsearchserviceeks_instance_config(
                service="fake_nrtsearchservice_service",
                instance="fake_instance",
                cluster="fake_cluster",
                load_deployments=True,
                soa_dir="/foo/bar",
            )
        )
        mock_load_v2_deployments_json.assert_called_with(
            service="fake_nrtsearchservice_service", soa_dir="/foo/bar"
        )
        mock_nrtsearchserviceeks_deployment_config.assert_called_with(
            service="fake_nrtsearchservice_service",
            instance="fake_instance",
            cluster="fake_cluster",
            config_dict=mock_config,
            branch_dict=mock_load_v2_deployments_json.return_value.get_branch_dict(),
            soa_dir="/foo/bar",
        )

        assert (
            nrtsearchserviceeks_deployment_config
            == mock_nrtsearchserviceeks_deployment_config.return_value
        )
