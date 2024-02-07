import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from paasta_tools.flink_eks_tools import DEFAULT_SOA_DIR
from paasta_tools.flink_eks_tools import FlinkEksDeploymentConfig
from paasta_tools.flink_eks_tools import load_flink_eks_instance_config
from paasta_tools.utils import BranchDictV2


class TestFlinkEksDeploymentConfig(unittest.TestCase):
    def test_flink_eks_deployment_config(self):
        config_dict = {"key": "value"}
        branch_dict = MagicMock(spec=BranchDictV2)
        flink_eks_deployment_config = FlinkEksDeploymentConfig(
            service="test_service",
            cluster="test_cluster",
            instance="test_instance",
            config_dict=config_dict,
            branch_dict=branch_dict,
        )

        self.assertEqual(flink_eks_deployment_config.service, "test_service")
        self.assertEqual(flink_eks_deployment_config.cluster, "test_cluster")
        self.assertEqual(flink_eks_deployment_config.instance, "test_instance")
        self.assertEqual(flink_eks_deployment_config.config_dict, config_dict)
        self.assertEqual(flink_eks_deployment_config.branch_dict, branch_dict)
        self.assertEqual(flink_eks_deployment_config.config_filename_prefix, "flinkeks")

    @patch("paasta_tools.flink_eks_tools.deep_merge_dictionaries")
    @patch("paasta_tools.flink_eks_tools.load_service_instance_config")
    @patch(
        "paasta_tools.flink_eks_tools.service_configuration_lib.read_service_configuration"
    )
    @patch("paasta_tools.flink_eks_tools.load_v2_deployments_json")
    def test_load_flink_eks_instance_config(
        self,
        mock_load_v2_deployments_json,
        mock_read_service_configuration,
        mock_load_service_instance_config,
        mock_deep_merge_dictionaries,
    ):
        mock_read_service_configuration.return_value = {"key": "value"}
        mock_load_service_instance_config.return_value = {
            "instance_key": "instance_value"
        }
        mock_load_v2_deployments_json.return_value = MagicMock(
            spec=["get_branch_dict"],
            get_branch_dict=MagicMock(return_value=MagicMock(spec=BranchDictV2)),
        )
        mock_deep_merge_dictionaries.return_value = {"merged_key": "merged_value"}

        flink_eks_deployment_config = load_flink_eks_instance_config(
            service="test_service",
            instance="test_instance",
            cluster="test_cluster",
        )

        self.assertIsInstance(flink_eks_deployment_config, FlinkEksDeploymentConfig)
        mock_read_service_configuration.assert_called_once_with(
            "test_service", soa_dir=DEFAULT_SOA_DIR
        )
        mock_load_service_instance_config.assert_called_once_with(
            "test_service",
            "test_instance",
            "flinkeks",
            "test_cluster",
            soa_dir=DEFAULT_SOA_DIR,
        )
        mock_load_v2_deployments_json.assert_called_once_with(
            "test_service", soa_dir=DEFAULT_SOA_DIR
        )
        mock_deep_merge_dictionaries.assert_called_once_with(
            overrides={"instance_key": "instance_value"}, defaults={"key": "value"}
        )


if __name__ == "__main__":
    unittest.main()
