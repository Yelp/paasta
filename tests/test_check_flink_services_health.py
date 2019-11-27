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
import pysensu_yelp
import pytest

from paasta_tools import check_flink_services_health
from paasta_tools import check_services_replication_tools
from paasta_tools.utils import compose_job_id

check_flink_services_health.log = mock.Mock()
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
        config_dict={},
    )
    mock_instance_config.get_replication_crit_percentage.return_value = 100
    mock_instance_config.get_registrations.return_value = [job_id]
    return mock_instance_config


def test_check_flink_service_health_healthy_service(instance_config):
    def check_under_replication_side_effect(*args, **kwargs):
        if kwargs["sub_component"] == "supervisor":
            return False, "foo"
        if kwargs["sub_component"] == "jobmanager":
            return False, "bar"
        if kwargs["sub_component"] == "taskmanager":
            return False, "baz"

    all_pods = []
    with mock.patch(
        "paasta_tools.check_flink_services_health.healthy_flink_containers_cnt",
        autospec=True,
        return_value=1,
    ), mock.patch(
        "paasta_tools.flink_tools.get_flink_jobmanager_overview",
        autospec=True,
        return_value={"taskmanagers": 3},
    ), mock.patch(
        "paasta_tools.check_flink_services_health.check_under_replication",
        autospec=True,
        side_effect=check_under_replication_side_effect,
    ) as mock_check_under_replication, mock.patch(
        "paasta_tools.check_flink_services_health.check_under_registered_taskmanagers",
        autospec=True,
        return_value=(False, "qux"),
    ) as mock_check_under_registered_taskmanagers, mock.patch(
        "paasta_tools.check_flink_services_health.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        instance_config.config_dict["taskmanager"] = {"instances": 3}
        check_flink_services_health.check_flink_service_health(
            instance_config=instance_config,
            all_pods=all_pods,
            smartstack_replication_checker=None,
        )
        expected = [
            mock.call(
                instance_config=instance_config,
                expected_count=1,
                num_available=1,
                sub_component="supervisor",
            ),
            mock.call(
                instance_config=instance_config,
                expected_count=1,
                num_available=1,
                sub_component="jobmanager",
            ),
            mock.call(
                instance_config=instance_config,
                expected_count=3,
                num_available=1,
                sub_component="taskmanager",
            ),
        ]
        mock_check_under_replication.assert_has_calls(expected)
        mock_check_under_registered_taskmanagers.assert_called_once_with(
            instance_config=instance_config,
            expected_count=3,
            num_reported=3,
            strerror=None,
        )
        mock_send_replication_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.OK,
            output="foo\n########\nbar\n########\nbaz\n########\nqux",
        )


def test_check_flink_service_health_too_few_taskmanagers(instance_config):
    def check_under_replication_side_effect(*args, **kwargs):
        if kwargs["sub_component"] == "supervisor":
            return False, "foo"
        if kwargs["sub_component"] == "jobmanager":
            return False, "bar"
        if kwargs["sub_component"] == "taskmanager":
            return True, "baz"

    all_pods = []
    with mock.patch(
        "paasta_tools.check_flink_services_health.healthy_flink_containers_cnt",
        autospec=True,
        return_value=1,
    ), mock.patch(
        "paasta_tools.check_flink_services_health._event_explanation",
        autospec=True,
        return_value="",
    ), mock.patch(
        "paasta_tools.flink_tools.get_flink_jobmanager_overview",
        autospec=True,
        return_value={"taskmanagers": 2},
    ), mock.patch(
        "paasta_tools.check_flink_services_health.check_under_replication",
        autospec=True,
        side_effect=check_under_replication_side_effect,
    ) as mock_check_under_replication, mock.patch(
        "paasta_tools.check_flink_services_health.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        instance_config.config_dict["taskmanager"] = {"instances": 3}
        check_flink_services_health.check_flink_service_health(
            instance_config=instance_config,
            all_pods=all_pods,
            smartstack_replication_checker=None,
        )
        expected = [
            mock.call(
                instance_config=instance_config,
                expected_count=1,
                num_available=1,
                sub_component="supervisor",
            ),
            mock.call(
                instance_config=instance_config,
                expected_count=1,
                num_available=1,
                sub_component="jobmanager",
            ),
            mock.call(
                instance_config=instance_config,
                expected_count=3,
                num_available=1,
                sub_component="taskmanager",
            ),
        ]
        mock_check_under_replication.assert_has_calls(expected)
        mock_send_replication_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output="foo\n########\nbar\n########\nbaz\n########\nService fake_service.fake_instance has 2 out of 3 expected instances of taskmanager reported by dashboard!\n(threshold: 100%)      paasta status -s fake_service -i fake_instance -c fake_cluster -vv\n",
        )


def _raise_dummy_exception(*args):
    raise ValueError("dummy exception")


def test_check_flink_service_health_dashboard_error(instance_config):
    def check_under_replication_side_effect(*args, **kwargs):
        if kwargs["sub_component"] == "supervisor":
            return False, "foo"
        if kwargs["sub_component"] == "jobmanager":
            return False, "bar"
        if kwargs["sub_component"] == "taskmanager":
            return False, "baz"

    all_pods = []
    with mock.patch(
        "paasta_tools.check_flink_services_health.healthy_flink_containers_cnt",
        autospec=True,
        return_value=1,
    ), mock.patch(
        "paasta_tools.check_flink_services_health._event_explanation",
        autospec=True,
        return_value="",
    ), mock.patch(
        "paasta_tools.flink_tools.get_flink_jobmanager_overview",
        side_effect=_raise_dummy_exception,
        autospec=True,
    ), mock.patch(
        "paasta_tools.check_flink_services_health.check_under_replication",
        autospec=True,
        side_effect=check_under_replication_side_effect,
    ) as mock_check_under_replication, mock.patch(
        "paasta_tools.check_flink_services_health.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        instance_config.config_dict["taskmanager"] = {"instances": 3}
        check_flink_services_health.check_flink_service_health(
            instance_config=instance_config,
            all_pods=all_pods,
            smartstack_replication_checker=None,
        )
        expected = [
            mock.call(
                instance_config=instance_config,
                expected_count=1,
                num_available=1,
                sub_component="supervisor",
            ),
            mock.call(
                instance_config=instance_config,
                expected_count=1,
                num_available=1,
                sub_component="jobmanager",
            ),
            mock.call(
                instance_config=instance_config,
                expected_count=3,
                num_available=1,
                sub_component="taskmanager",
            ),
        ]
        mock_check_under_replication.assert_has_calls(expected)
        mock_send_replication_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output="foo\n########\nbar\n########\nbaz\n########\nDashboard of service fake_service.fake_instance is not available!\n(dummy exception)      paasta status -s fake_service -i fake_instance -c fake_cluster -vv\n",
        )
