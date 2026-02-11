# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from unittest import mock

from paasta_tools import adhoc_tools
from paasta_tools.utils import DeploymentsJsonV2
from paasta_tools.utils import NoConfigurationForServiceError


def test_get_default_interactive_config():
    with mock.patch(
        "paasta_tools.adhoc_tools.load_adhoc_job_config", autospec=True
    ) as mock_load_adhoc_job_config:
        mock_load_adhoc_job_config.return_value = adhoc_tools.AdhocJobConfig(
            service="fake_service",
            instance="interactive",
            cluster="fake_cluster",
            config_dict={},
            branch_dict={"deploy_group": "fake_deploy_group"},
        )
        result = adhoc_tools.get_default_interactive_config(
            "fake_service", "fake_cluster", "/fake/soa/dir", load_deployments=False
        )
        assert result.get_cpus() == 4
        assert result.get_mem() == 10240
        assert result.get_disk() == 1024


def test_get_default_interactive_config_reads_from_tty():
    with mock.patch(
        "paasta_tools.adhoc_tools.prompt_pick_one", autospec=True
    ) as mock_prompt_pick_one, mock.patch(
        "paasta_tools.adhoc_tools.load_adhoc_job_config", autospec=True
    ) as mock_load_adhoc_job_config, mock.patch(
        "paasta_tools.adhoc_tools.load_v2_deployments_json", autospec=True
    ) as mock_load_deployments_json:
        mock_prompt_pick_one.return_value = "fake_deploygroup"
        mock_load_adhoc_job_config.side_effect = NoConfigurationForServiceError
        mock_load_deployments_json.return_value = DeploymentsJsonV2(
            service="fake-service",
            config_dict={
                "deployments": {
                    "fake_deploygroup": {
                        "docker_image": mock.sentinel.docker_image,
                        "git_sha": mock.sentinel.git_sha,
                    }
                },
                "controls": {},
            },
        )
        result = adhoc_tools.get_default_interactive_config(
            "fake_service", "fake_cluster", "/fake/soa/dir", load_deployments=True
        )
        assert result.get_deploy_group() == "fake_deploygroup"
        assert result.get_docker_image() == mock.sentinel.docker_image


def test_adhoc_config_node_selectors_in_pod_spec():
    """Test that node_selectors from adhoc config appear in the generated pod spec."""
    with mock.patch(
        "paasta_tools.kubernetes_tools.load_system_paasta_config", autospec=True
    ) as mock_load_system_config, mock.patch(
        "paasta_tools.utils.load_system_paasta_config", autospec=True
    ) as mock_load_system_config_utils:
        mock_load_system_config.return_value.get_cluster_aliases.return_value = []
        mock_load_system_config.return_value.get_volumes.return_value = []
        mock_load_system_config.return_value.get_dockercfg_location.return_value = (
            "file:///root/.dockercfg"
        )
        # Copy the mock to utils as well
        mock_load_system_config_utils.return_value = (
            mock_load_system_config.return_value
        )

        # Create an adhoc config with node_selectors
        adhoc_config = adhoc_tools.AdhocJobConfig(
            service="test_service",
            instance="test_instance",
            cluster="test_cluster",
            config_dict={
                "cpus": 1.0,
                "mem": 1024,
                "disk": 1024,
                "node_selectors": {
                    "instance_type": "c5.xlarge",
                    "custom_label": [
                        {"operator": "In", "values": ["value1", "value2"]}
                    ],
                },
            },
            branch_dict={
                "docker_image": "docker-dev.yelpcorp.com/test:latest",
                "git_sha": "abc123",
                "desired_state": "start",
                "force_bounce": None,
            },
        )

        # Wrap in EksDeploymentConfig like remote_run does
        from paasta_tools.eks_tools import EksDeploymentConfig

        eks_config = EksDeploymentConfig(
            service=adhoc_config.service,
            cluster=adhoc_config.cluster,
            instance=adhoc_config.instance,
            config_dict=adhoc_config.config_dict,
            branch_dict=adhoc_config.branch_dict,
            soa_dir="/fake/soa/dir",
        )

        # Generate the pod template spec
        pod_template_spec = eks_config.get_pod_template_spec(
            git_sha="abc123",
            system_paasta_config=mock_load_system_config.return_value,
        )

        # Verify node selectors are in the pod spec
        # instance_type gets translated to canonical kubernetes label
        assert (
            pod_template_spec.spec.node_selector["node.kubernetes.io/instance-type"]
            == "c5.xlarge"
        )

        # Complex selector should be in node affinity
        assert pod_template_spec.spec.affinity is not None
        assert pod_template_spec.spec.affinity.node_affinity is not None
        node_affinity = pod_template_spec.spec.affinity.node_affinity
        assert (
            node_affinity.required_during_scheduling_ignored_during_execution
            is not None
        )
        node_selector_terms = (
            node_affinity.required_during_scheduling_ignored_during_execution.node_selector_terms
        )
        assert len(node_selector_terms) > 0

        # Find the custom_label requirement
        all_requirements = []
        for term in node_selector_terms:
            all_requirements.extend(term.match_expressions)

        custom_label_requirements = [
            req for req in all_requirements if req.key == "custom_label"
        ]
        assert len(custom_label_requirements) > 0
        req = custom_label_requirements[0]
        assert req.operator == "In"
        assert set(req.values) == {"value1", "value2"}
