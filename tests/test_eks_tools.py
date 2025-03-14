from unittest import mock

from paasta_tools.eks_tools import agnostic_load_service_config
from paasta_tools.eks_tools import load_eks_service_config
from paasta_tools.eks_tools import load_eks_service_config_no_cache
from paasta_tools.utils import DEFAULT_SOA_DIR


def test_load_eks_service_config_no_cache():
    with mock.patch(
        "service_configuration_lib.read_service_configuration", autospec=True
    ) as mock_read_service_configuration, mock.patch(
        "paasta_tools.eks_tools.load_service_instance_config", autospec=True
    ) as mock_load_service_instance_config, mock.patch(
        "paasta_tools.eks_tools.load_v2_deployments_json", autospec=True
    ) as mock_load_v2_deployments_json, mock.patch(
        "paasta_tools.eks_tools.EksDeploymentConfig", autospec=True
    ) as mock_eks_deploy_config:
        mock_config = {"freq": "108.9"}
        mock_load_service_instance_config.return_value = mock_config
        mock_read_service_configuration.return_value = {}
        ret = load_eks_service_config_no_cache(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            load_deployments=False,
            soa_dir="/nail/blah",
        )
        mock_load_service_instance_config.assert_called_with(
            service="kurupt",
            instance="fm",
            instance_type="eks",
            cluster="brentford",
            soa_dir="/nail/blah",
        )
        mock_eks_deploy_config.assert_called_with(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            config_dict={"freq": "108.9"},
            branch_dict=None,
            soa_dir="/nail/blah",
        )
        assert not mock_load_v2_deployments_json.called
        assert ret == mock_eks_deploy_config.return_value

        mock_eks_deploy_config.reset_mock()
        ret = load_eks_service_config_no_cache(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            load_deployments=True,
            soa_dir="/nail/blah",
        )
        mock_load_v2_deployments_json.assert_called_with(
            service="kurupt", soa_dir="/nail/blah"
        )
        mock_eks_deploy_config.assert_called_with(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            config_dict={"freq": "108.9"},
            branch_dict=mock_load_v2_deployments_json.return_value.get_branch_dict(),
            soa_dir="/nail/blah",
        )
        assert ret == mock_eks_deploy_config.return_value


def test_load_eks_service_config():
    with mock.patch(
        "paasta_tools.eks_tools.load_eks_service_config_no_cache",
        autospec=True,
    ) as mock_load_kubernetes_service_config_no_cache:
        ret = load_eks_service_config(
            service="kurupt",
            instance="fm",
            cluster="brentford",
            load_deployments=True,
            soa_dir="/nail/blah",
        )
        assert ret == mock_load_kubernetes_service_config_no_cache.return_value


@mock.patch("paasta_tools.eks_tools.load_eks_service_config_no_cache", autospec=True)
@mock.patch(
    "paasta_tools.eks_tools.load_kubernetes_service_config_no_cache", autospec=True
)
@mock.patch("paasta_tools.eks_tools.validate_service_instance", autospec=True)
def test_agnostic_load_service_config(mock_validate, mock_load_kube, mock_load_eks):
    # kube config
    mock_validate.return_value = "kube"
    assert (
        agnostic_load_service_config("foo", "bar", "dev") == mock_load_kube.return_value
    )
    mock_load_eks.assert_not_called()
    mock_load_kube.assert_called_once_with("foo", "bar", "dev", True, DEFAULT_SOA_DIR)
    # eks config
    mock_load_kube.reset_mock()
    mock_validate.return_value = "eks"
    assert (
        agnostic_load_service_config("biz", "buz", "dev") == mock_load_eks.return_value
    )
    mock_load_kube.assert_not_called()
    mock_load_eks.assert_called_once_with("biz", "buz", "dev", True, DEFAULT_SOA_DIR)
