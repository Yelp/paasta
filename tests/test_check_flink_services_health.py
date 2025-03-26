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
from paasta_tools.check_flink_services_health import check_under_registered_taskmanagers
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


@mock.patch(
    "paasta_tools.flink_tools.get_flink_jobmanager_overview",
    autospec=True,
    return_value={"taskmanagers": 3},
)
def test_check_under_registered_taskmanagers_ok(mock_overview, instance_config):
    under, output, description = check_under_registered_taskmanagers(
        instance_config,
        expected_count=3,
        cr_name="fake--service-575c857546",
        is_eks=False,
    )
    assert not under
    assert (
        "fake_service.fake_instance has 3/3 taskmanagers "
        "reported by dashboard (threshold: 100%)"
    ) in output
    assert "fake_service.fake_instance taskmanager is available" in description


@mock.patch(
    "paasta_tools.flink_tools.get_flink_jobmanager_overview",
    autospec=True,
    return_value={"taskmanagers": 2},
)
def test_check_under_registered_taskmanagers_under(mock_overview, instance_config):
    under, output, description = check_under_registered_taskmanagers(
        instance_config,
        expected_count=3,
        cr_name="fake--service-575c857546",
        is_eks=False,
    )
    assert under
    assert (
        "fake_service.fake_instance has 2/3 taskmanagers "
        "reported by dashboard (threshold: 100%)"
    ) in output
    assert (
        "paasta status -s fake_service -i fake_instance -c fake_cluster -vv"
        in description
    )


@mock.patch(
    "paasta_tools.flink_tools.get_flink_jobmanager_overview",
    autospec=True,
    side_effect=ValueError("dummy exception"),
)
def test_check_under_registered_taskmanagers_error(mock_overview, instance_config):
    under, output, description = check_under_registered_taskmanagers(
        instance_config,
        expected_count=3,
        cr_name="fake--service-575c857546",
        is_eks=False,
    )
    assert under
    assert (
        "Dashboard of service fake_service.fake_instance is not available "
        "(dummy exception)"
    ) in output
    assert (
        "paasta status -s fake_service -i fake_instance -c fake_cluster -vv"
        in description
    )


def test_check_flink_service_health_healthy(instance_config):
    with mock.patch(
        "paasta_tools.check_flink_services_health.healthy_flink_containers_cnt",
        autospec=True,
        return_value=1,
    ), mock.patch(
        "paasta_tools.check_flink_services_health.check_under_replication",
        autospec=True,
        return_value=(False, "OK", "check_rep"),
    ) as mock_check_under_replication, mock.patch(
        "paasta_tools.check_flink_services_health.check_under_registered_taskmanagers",
        autospec=True,
        return_value=(False, "OK", "check_task"),
    ) as mock_check_under_registered_taskmanagers, mock.patch(
        "paasta_tools.check_flink_services_health.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        instance_config.config_dict["taskmanager"] = {"instances": 3}
        check_flink_services_health.check_flink_service_health(
            instance_config=instance_config,
            pods_by_service_instance={},
            replication_checker=None,
            dry_run=True,
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
            instance_config=instance_config, expected_count=3, cr_name="", is_eks=False
        )
        mock_send_replication_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.OK,
            output="OK, OK, OK, OK",
            description="check_rep\n########\ncheck_rep\n########\ncheck_rep\n########\ncheck_task",
            dry_run=True,
        )


def test_check_flink_service_health_too_few_taskmanagers(instance_config):
    def check_under_replication_side_effect(*args, **kwargs):
        if kwargs["sub_component"] == "supervisor":
            return False, "OK", "check_rep"
        if kwargs["sub_component"] == "jobmanager":
            return False, "OK", "check_rep"
        if kwargs["sub_component"] == "taskmanager":
            return True, "NOPE", "check_rep"

    with mock.patch(
        "paasta_tools.check_flink_services_health.healthy_flink_containers_cnt",
        autospec=True,
        return_value=1,
    ), mock.patch(
        "paasta_tools.check_flink_services_health.check_under_registered_taskmanagers",
        autospec=True,
        return_value=(True, "NOPE", "check_task"),
    ) as mock_check_under_registered_taskmanagers, mock.patch(
        "paasta_tools.check_flink_services_health.check_under_replication",
        autospec=True,
        side_effect=check_under_replication_side_effect,
    ) as mock_check_under_replication, mock.patch(
        "paasta_tools.check_flink_services_health.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        instance_config.config_dict["taskmanager"] = {"instances": 3}
        check_flink_services_health.check_flink_service_health(
            instance_config=instance_config,
            pods_by_service_instance={},
            replication_checker=None,
            dry_run=True,
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
            instance_config=instance_config, expected_count=3, cr_name="", is_eks=False
        )
        mock_send_replication_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output="OK, OK, NOPE, NOPE",
            description="check_rep\n########\ncheck_rep\n########\ncheck_rep\n########\ncheck_task",
            dry_run=True,
        )


def test_check_flink_service_health_under_registered_taskamanagers(instance_config):
    with mock.patch(
        "paasta_tools.check_flink_services_health.healthy_flink_containers_cnt",
        autospec=True,
        return_value=1,
    ), mock.patch(
        "paasta_tools.check_flink_services_health.check_under_replication",
        autospec=True,
        return_value=(False, "OK", "check_rep"),
    ) as mock_check_under_replication, mock.patch(
        "paasta_tools.check_flink_services_health.check_under_registered_taskmanagers",
        autospec=True,
        return_value=(True, "NOPE", "check_task"),
    ) as mock_check_under_registered_taskmanagers, mock.patch(
        "paasta_tools.check_flink_services_health.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        instance_config.config_dict["taskmanager"] = {"instances": 3}
        check_flink_services_health.check_flink_service_health(
            instance_config=instance_config,
            pods_by_service_instance={},
            replication_checker=None,
            dry_run=True,
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
            cr_name="",
            is_eks=False,
        )
        mock_send_replication_event.assert_called_once_with(
            instance_config=instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output="OK, OK, OK, NOPE",
            description="check_rep\n########\ncheck_rep\n########\ncheck_rep\n########\ncheck_task",
            dry_run=True,
        )
