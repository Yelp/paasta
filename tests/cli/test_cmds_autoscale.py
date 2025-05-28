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
import mock
import pytest

from paasta_tools.cli.cmds.autoscale import paasta_autoscale
from paasta_tools.eks_tools import EksDeploymentConfig
from paasta_tools.kubernetes_tools import KubernetesDeploymentConfig


@mock.patch("paasta_tools.cli.cmds.autoscale.figure_out_service_name", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.autoscale.client.get_paasta_oapi_client", autospec=True
)
@mock.patch("paasta_tools.cli.cmds.autoscale._log_audit", autospec=True)
@pytest.mark.parametrize(
    "instance_type_class",
    (
        EksDeploymentConfig,
        KubernetesDeploymentConfig,
    ),
)
def test_paasta_autoscale(
    mock__log_audit,
    mock_get_paasta_oapi_client,
    mock_figure_out_service_name,
    instance_type_class,
):
    service = "fake_service"
    instance = "fake_instance"
    cluster = "fake_cluster"

    mock_figure_out_service_name.return_value = service
    mock_api = mock.Mock()
    mock_get_paasta_oapi_client.return_value = mock.Mock(autoscaler=mock_api)

    args = mock.MagicMock()
    args.service = service
    args.clusters = cluster
    args.instances = instance
    args.set = 14
    # this is kinda silly, but Mocks are truthy by default - without this, the underlying code thinks that both --set and --set-min are both set
    args.set_min = None
    args.duration = None

    mock_api.update_autoscaler_count.return_value = (
        mock.Mock(desired_instances=14),
        200,
        None,
    )
    mock__log_audit.return_value = None

    with mock.patch(
        "paasta_tools.cli.cmds.autoscale.get_instance_configs_for_service",
        return_value=iter([mock.Mock(__class__=instance_type_class)]),
        autospec=True,
    ):
        paasta_autoscale(args)
    assert mock_api.update_autoscaler_count.call_count == 1


@mock.patch("paasta_tools.cli.cmds.autoscale.figure_out_service_name", autospec=True)
@mock.patch(
    "paasta_tools.cli.cmds.autoscale.client.get_paasta_oapi_client", autospec=True
)
@mock.patch("paasta_tools.cli.cmds.autoscale._log_audit", autospec=True)
def test_paasta_autoscale_no_config(
    mock__log_audit,
    mock_get_paasta_oapi_client,
    mock_figure_out_service_name,
):
    service = "fake_service"
    instance = "fake_instance"
    cluster = "fake_cluster"

    mock_figure_out_service_name.return_value = service
    mock_api = mock.Mock()
    mock_get_paasta_oapi_client.return_value = mock.Mock(autoscaler=mock_api)

    args = mock.MagicMock()
    args.service = service
    args.clusters = cluster
    args.instances = instance
    args.set = 14

    with mock.patch(
        "paasta_tools.cli.cmds.autoscale.get_instance_configs_for_service",
        return_value=iter(()),
        autospec=True,
    ):
        assert paasta_autoscale(args) == 1
