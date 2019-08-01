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

from paasta_tools import list_marathon_service_instances
from paasta_tools.marathon_tools import MarathonClients
from paasta_tools.mesos.exceptions import NoSlavesAvailableError


def test_get_desired_marathon_configs():
    with mock.patch(
        "paasta_tools.list_marathon_service_instances.get_services_for_cluster",
        autospec=True,
    ) as mock_get_services_for_cluster, mock.patch(
        "paasta_tools.list_marathon_service_instances.load_marathon_service_config",
        autospec=True,
    ) as mock_load_marathon_service_config, mock.patch(
        "paasta_tools.list_marathon_service_instances.load_system_paasta_config",
        autospec=True,
    ), mock.patch(
        "paasta_tools.list_marathon_service_instances._log", autospec=True
    ):
        mock_app_dict = {"id": "/service.instance.git.configs"}
        mock_app = mock.MagicMock(
            format_marathon_app_dict=mock.MagicMock(return_value=mock_app_dict)
        )
        mock_app_2 = mock.MagicMock(
            format_marathon_app_dict=mock.MagicMock(side_effect=[Exception])
        )
        mock_get_services_for_cluster.return_value = [
            ("service", "instance"),
            ("service", "broken_instance"),
        ]
        mock_load_marathon_service_config.side_effect = [mock_app, mock_app_2]
        assert list_marathon_service_instances.get_desired_marathon_configs(
            "/fake/soa/dir"
        ) == (
            {"service.instance.git.configs": mock_app_dict},
            {"service.instance.git.configs": mock_app},
        )


def test_get_desired_marathon_configs_handles_no_slaves():
    with mock.patch(
        "paasta_tools.list_marathon_service_instances.get_services_for_cluster",
        autospec=True,
    ), mock.patch(
        "paasta_tools.list_marathon_service_instances.load_marathon_service_config",
        autospec=True,
    ), mock.patch(
        "paasta_tools.list_marathon_service_instances.load_system_paasta_config",
        autospec=True,
    ) as mock_load_marathon_service_config:
        mock_load_marathon_service_config.return_value = mock.MagicMock(
            format_marathon_app_dict=mock.MagicMock(
                side_effect=NoSlavesAvailableError()
            )
        )
        assert list_marathon_service_instances.get_desired_marathon_configs(
            "/fake/soadir/"
        ) == ({}, {})


def test_get_service_instances_that_need_bouncing():
    with mock.patch(
        "paasta_tools.list_marathon_service_instances.get_desired_marathon_configs",
        autospec=True,
    ) as mock_get_desired_marathon_configs, mock.patch(
        "paasta_tools.list_marathon_service_instances.get_num_at_risk_tasks",
        autospec=True,
    ) as mock_get_num_at_risk_tasks, mock.patch(
        "paasta_tools.list_marathon_service_instances.get_draining_hosts", autospec=True
    ):
        mock_get_desired_marathon_configs.return_value = (
            {
                "fake--service.fake--instance.sha.config": {"instances": 5},
                "fake--service2.fake--instance.sha.config": {"instances": 5},
            },
            {
                "fake--service.fake--instance.sha.config": mock.Mock(
                    get_marathon_shard=mock.Mock(return_value=None)
                ),
                "fake--service2.fake--instance.sha.config": mock.Mock(
                    get_marathon_shard=mock.Mock(return_value=None)
                ),
            },
        )

        fake_apps = [
            mock.MagicMock(instances=5, id="/fake--service.fake--instance.sha.config2"),
            mock.MagicMock(instances=5, id="/fake--service2.fake--instance.sha.config"),
        ]
        mock_client = mock.MagicMock(list_apps=mock.MagicMock(return_value=fake_apps))
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])

        mock_get_num_at_risk_tasks.return_value = 0
        assert set(
            list_marathon_service_instances.get_service_instances_that_need_bouncing(
                marathon_clients=fake_clients, soa_dir="/fake/soa/dir"
            )
        ) == {"fake_service.fake_instance"}


def test_get_service_instances_that_need_bouncing_two_existing_services():
    with mock.patch(
        "paasta_tools.list_marathon_service_instances.get_desired_marathon_configs",
        autospec=True,
    ) as mock_get_desired_marathon_configs, mock.patch(
        "paasta_tools.list_marathon_service_instances.get_num_at_risk_tasks",
        autospec=True,
    ) as mock_get_num_at_risk_tasks, mock.patch(
        "paasta_tools.list_marathon_service_instances.get_draining_hosts", autospec=True
    ):
        mock_get_desired_marathon_configs.return_value = (
            {"fake--service.fake--instance.sha.config": {"instances": 5}},
            {
                "fake--service.fake--instance.sha.config": mock.Mock(
                    get_marathon_shard=mock.Mock(return_value=None)
                )
            },
        )
        fake_apps = [
            mock.MagicMock(instances=5, id="/fake--service.fake--instance.sha.config"),
            mock.MagicMock(instances=5, id="/fake--service.fake--instance.sha.config2"),
        ]
        mock_client = mock.MagicMock(list_apps=mock.MagicMock(return_value=fake_apps))
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        mock_get_num_at_risk_tasks.return_value = 0
        assert set(
            list_marathon_service_instances.get_service_instances_that_need_bouncing(
                marathon_clients=fake_clients, soa_dir="/fake/soa/dir"
            )
        ) == {"fake_service.fake_instance"}


def test_get_service_instances_that_need_bouncing_no_difference():
    with mock.patch(
        "paasta_tools.list_marathon_service_instances.get_desired_marathon_configs",
        autospec=True,
    ) as mock_get_desired_marathon_configs, mock.patch(
        "paasta_tools.list_marathon_service_instances.get_num_at_risk_tasks",
        autospec=True,
    ) as mock_get_num_at_risk_tasks, mock.patch(
        "paasta_tools.list_marathon_service_instances.get_draining_hosts", autospec=True
    ):
        mock_get_desired_marathon_configs.return_value = (
            {"fake--service.fake--instance.sha.config": {"instances": 5}},
            {
                "fake--service.fake--instance.sha.config": mock.Mock(
                    get_marathon_shard=mock.Mock(return_value=None)
                )
            },
        )
        fake_apps = [
            mock.MagicMock(instances=5, id="/fake--service.fake--instance.sha.config")
        ]
        mock_client = mock.MagicMock(list_apps=mock.MagicMock(return_value=fake_apps))
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        mock_get_num_at_risk_tasks.return_value = 0
        assert (
            set(
                list_marathon_service_instances.get_service_instances_that_need_bouncing(
                    marathon_clients=fake_clients, soa_dir="/fake/soa/dir"
                )
            )
            == set()
        )


def test_get_service_instances_that_need_bouncing_instances_difference():
    with mock.patch(
        "paasta_tools.list_marathon_service_instances.get_desired_marathon_configs",
        autospec=True,
    ) as mock_get_desired_marathon_configs, mock.patch(
        "paasta_tools.list_marathon_service_instances.get_num_at_risk_tasks",
        autospec=True,
    ) as mock_get_num_at_risk_tasks, mock.patch(
        "paasta_tools.list_marathon_service_instances.get_draining_hosts", autospec=True
    ):
        mock_get_desired_marathon_configs.return_value = (
            {"fake--service.fake--instance.sha.config": {"instances": 5}},
            {
                "fake--service.fake--instance.sha.config": mock.Mock(
                    get_marathon_shard=mock.Mock(return_value=None)
                )
            },
        )
        fake_apps = [
            mock.MagicMock(instances=4, id="/fake--service.fake--instance.sha.config")
        ]
        mock_client = mock.MagicMock(list_apps=mock.MagicMock(return_value=fake_apps))
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        mock_get_num_at_risk_tasks.return_value = 0
        assert set(
            list_marathon_service_instances.get_service_instances_that_need_bouncing(
                marathon_clients=fake_clients, soa_dir="/fake/soa/dir"
            )
        ) == {"fake_service.fake_instance"}


def test_get_service_instances_that_need_bouncing_at_risk():
    with mock.patch(
        "paasta_tools.list_marathon_service_instances.get_desired_marathon_configs",
        autospec=True,
    ) as mock_get_desired_marathon_configs, mock.patch(
        "paasta_tools.list_marathon_service_instances.get_num_at_risk_tasks",
        autospec=True,
    ) as mock_get_num_at_risk_tasks, mock.patch(
        "paasta_tools.list_marathon_service_instances.get_draining_hosts", autospec=True
    ):
        mock_get_desired_marathon_configs.return_value = (
            {"fake--service.fake--instance.sha.config": {"instances": 5}},
            {
                "fake--service.fake--instance.sha.config": mock.Mock(
                    get_marathon_shard=mock.Mock(return_value=None)
                )
            },
        )
        fake_apps = [
            mock.MagicMock(instances=5, id="/fake--service.fake--instance.sha.config")
        ]
        mock_client = mock.MagicMock(list_apps=mock.MagicMock(return_value=fake_apps))
        fake_clients = MarathonClients(current=[mock_client], previous=[mock_client])
        mock_get_num_at_risk_tasks.return_value = 1
        assert set(
            list_marathon_service_instances.get_service_instances_that_need_bouncing(
                marathon_clients=fake_clients, soa_dir="/fake/soa/dir"
            )
        ) == {"fake_service.fake_instance"}


def test_exceptions_logged_with_debug():
    with mock.patch(
        "paasta_tools.list_marathon_service_instances.get_services_for_cluster",
        autospec=True,
    ) as mock_get_services_for_cluster, mock.patch(
        "paasta_tools.list_marathon_service_instances.load_marathon_service_config",
        autospec=True,
    ) as mock_load_marathon_service_config, mock.patch(
        "paasta_tools.list_marathon_service_instances.load_system_paasta_config",
        autospec=True,
    ), mock.patch(
        "paasta_tools.list_marathon_service_instances._log", autospec=True
    ) as mock_log:
        mock_app = mock.MagicMock(
            format_marathon_app_dict=mock.MagicMock(side_effect=[Exception])
        )
        mock_get_services_for_cluster.return_value = [("service", "broken_instance")]
        mock_load_marathon_service_config.side_effect = [mock_app]
        list_marathon_service_instances.get_desired_marathon_configs("/fake/soa/dir")
        mock_log.assert_called_once_with(
            service="service",
            line=mock.ANY,
            component="deploy",
            level="debug",
            cluster=mock.ANY,
            instance="broken_instance",
        )
