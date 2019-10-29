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

from paasta_tools import check_kubernetes_services_replication
from paasta_tools import check_services_replication_tools
from paasta_tools.utils import compose_job_id

check_kubernetes_services_replication.log = mock.Mock()
check_services_replication_tools.log = mock.Mock()


@pytest.fixture
def instance_config():
    service = "fake_service"
    instance = "fake_instance"
    job_id = compose_job_id(service, instance)
    mock_instance_config = mock.Mock(
        service=service,
        instance=instance,
        cluster="fake_cluster",
        soa_dir="fake_soa_dir",
        job_id=job_id,
    )
    mock_instance_config.get_replication_crit_percentage.return_value = 90
    mock_instance_config.get_registrations.return_value = [job_id]
    return mock_instance_config


def test_check_service_replication_for_normal_smartstack(instance_config):
    instance_config.get_instances.return_value = 100
    all_pods = []
    with mock.patch(
        "paasta_tools.check_kubernetes_services_replication.get_proxy_port_for_instance",
        autospec=True,
        return_value=666,
    ), mock.patch(
        "paasta_tools.monitoring_tools.check_smartstack_replication_for_instance",
        autospec=True,
    ) as mock_check_smartstack_replication_for_service:
        check_kubernetes_services_replication.check_kubernetes_pod_replication(
            instance_config=instance_config,
            all_pods=all_pods,
            smartstack_replication_checker=None,
        )
        mock_check_smartstack_replication_for_service.assert_called_once_with(
            instance_config=instance_config,
            expected_count=100,
            smartstack_replication_checker=None,
        )


def test_check_service_replication_for_smartstack_with_different_namespace(
    instance_config,
):
    instance_config.get_instances.return_value = 100
    all_pods = []
    with mock.patch(
        "paasta_tools.check_kubernetes_services_replication.get_proxy_port_for_instance",
        autospec=True,
        return_value=666,
    ), mock.patch(
        "paasta_tools.monitoring_tools.check_smartstack_replication_for_instance",
        autospec=True,
    ) as mock_check_smartstack_replication_for_service, mock.patch(
        "paasta_tools.check_kubernetes_services_replication.check_healthy_kubernetes_tasks_for_service_instance",
        autospec=True,
    ) as mock_check_healthy_kubernetes_tasks:
        instance_config.get_registrations.return_value = ["some-random-other-namespace"]
        check_kubernetes_services_replication.check_kubernetes_pod_replication(
            instance_config=instance_config,
            all_pods=all_pods,
            smartstack_replication_checker=None,
        )
        assert not mock_check_smartstack_replication_for_service.called
        mock_check_healthy_kubernetes_tasks.assert_called_once_with(
            instance_config=instance_config, expected_count=100, all_pods=[]
        )


def test_check_service_replication_for_non_smartstack(instance_config):
    instance_config.get_instances.return_value = 100

    with mock.patch(
        "paasta_tools.check_kubernetes_services_replication.get_proxy_port_for_instance",
        autospec=True,
        return_value=None,
    ), mock.patch(
        "paasta_tools.check_kubernetes_services_replication.check_healthy_kubernetes_tasks_for_service_instance",
        autospec=True,
    ) as mock_check_healthy_kubernetes_tasks:
        check_kubernetes_services_replication.check_kubernetes_pod_replication(
            instance_config=instance_config,
            all_pods=[],
            smartstack_replication_checker=None,
        )
        mock_check_healthy_kubernetes_tasks.assert_called_once_with(
            instance_config=instance_config, expected_count=100, all_pods=[]
        )


def test_check_all_kubernetes_services_replication(instance_config):
    soa_dir = "anw"
    instance_config.get_docker_image.return_value = True
    with mock.patch(
        "paasta_tools.check_services_replication_tools.list_services",
        autospec=True,
        return_value=["a"],
    ), mock.patch(
        "paasta_tools.check_kubernetes_services_replication.check_kubernetes_pod_replication",
        autospec=True,
    ) as mock_check_service_replication, mock.patch(
        "paasta_tools.check_services_replication_tools.load_system_paasta_config",
        autospec=True,
    ) as mock_load_system_paasta_config, mock.patch(
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
        mock_load_system_paasta_config.return_value.get_cluster = mock.Mock(
            return_value="fake_cluster"
        )
        check_services_replication_tools.check_all_kubernetes_based_services_replication(
            soa_dir=soa_dir,
            service_instances=[],
            instance_type_class=None,
            check_service_replication=mock_check_service_replication,
            namespace="baz",
        )
        mock_paasta_service_config_loader.assert_called_once_with(
            service="a", soa_dir=soa_dir
        )
        instance_config.get_docker_image.assert_called_once_with()
        assert mock_check_service_replication.called


def test_main():
    with mock.patch(
        "paasta_tools.check_services_replication_tools.check_all_kubernetes_based_services_replication",
        autospec=True,
    ) as mock_check_all_kubernetes_services_replication, mock.patch(
        "paasta_tools.check_services_replication_tools.parse_args", autospec=True
    ):
        check_kubernetes_services_replication.main(
            instance_type_class=None, check_service_replication=None, namespace="baz"
        )
        assert mock_check_all_kubernetes_services_replication.called


def test_check_healthy_kubernetes_tasks_for_service_instance():
    with mock.patch(
        "paasta_tools.check_kubernetes_services_replication.filter_pods_by_service_instance",
        autospec=True,
    ) as mock_filter_pods_by_service_instance, mock.patch(
        "paasta_tools.check_kubernetes_services_replication.is_pod_ready",
        autospec=True,
        side_effect=[True, False],
    ), mock.patch(
        "paasta_tools.monitoring_tools.send_replication_event_if_under_replication",
        autospec=True,
    ) as mock_send_replication_event_if_under_replication:
        mock_instance_config = mock.Mock()
        mock_pods = mock.Mock()
        mock_pod_1 = mock.Mock()
        mock_pod_2 = mock.Mock()
        mock_filter_pods_by_service_instance.return_value = [mock_pod_1, mock_pod_2]
        check_kubernetes_services_replication.check_healthy_kubernetes_tasks_for_service_instance(
            mock_instance_config, 5, mock_pods
        )
        mock_filter_pods_by_service_instance.assert_called_with(
            pod_list=mock_pods,
            service=mock_instance_config.service,
            instance=mock_instance_config.instance,
        )
        mock_send_replication_event_if_under_replication.assert_called_with(
            instance_config=mock_instance_config, expected_count=5, num_available=1
        )
