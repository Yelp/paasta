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

from paasta_tools import check_flink_services_health
from paasta_tools import check_services_replication_tools
from paasta_tools.check_flink_services_health import check_under_registered_taskmanagers

check_flink_services_health.log = mock.Mock()
check_services_replication_tools.log = mock.Mock()


@mock.patch(
    "paasta_tools.flink_tools.get_flink_jobmanager_overview",
    autospec=True,
    return_value={"taskmanagers": 3},
)
def test_check_under_registered_taskmanagers_ok(mock_overview, flink_instance_config):
    under, output = check_under_registered_taskmanagers(
        flink_instance_config, expected_count=3
    )
    assert not under
    assert (
        "Service fake_service.fake_instance has 3 out of 3 expected instances of "
        "taskmanager reported by dashboard!\n"
        "(threshold: 100%)"
    ) in output


@mock.patch(
    "paasta_tools.flink_tools.get_flink_jobmanager_overview",
    autospec=True,
    return_value={"taskmanagers": 2},
)
def test_check_under_registered_taskmanagers_under(
    mock_overview, flink_instance_config
):
    under, output = check_under_registered_taskmanagers(
        flink_instance_config, expected_count=3
    )
    assert under
    assert (
        "Service fake_service.fake_instance has 2 out of 3 expected instances of "
        "taskmanager reported by dashboard!\n"
        "(threshold: 100%)"
    ) in output
    assert (
        "paasta status -s fake_service -i fake_instance -c fake_cluster -vv" in output
    )


@mock.patch(
    "paasta_tools.flink_tools.get_flink_jobmanager_overview",
    autospec=True,
    side_effect=ValueError("dummy exception"),
)
def test_check_under_registered_taskmanagers_error(
    mock_overview, flink_instance_config
):
    under, output = check_under_registered_taskmanagers(
        flink_instance_config, expected_count=3
    )
    assert under
    assert (
        "Dashboard of service fake_service.fake_instance is not available!\n"
        "(dummy exception)\n"
        "What this alert"
    ) in output
    assert (
        "paasta status -s fake_service -i fake_instance -c fake_cluster -vv" in output
    )


def test_check_flink_service_health_healthy(flink_instance_config):
    all_pods = []
    with mock.patch(
        "paasta_tools.check_flink_services_health.healthy_flink_containers_cnt",
        autospec=True,
        return_value=1,
    ), mock.patch(
        "paasta_tools.check_flink_services_health.check_under_replication",
        autospec=True,
        return_value=(False, "OK"),
    ) as mock_check_under_replication, mock.patch(
        "paasta_tools.check_flink_services_health.check_under_registered_taskmanagers",
        autospec=True,
        return_value=(False, "OK"),
    ) as mock_check_under_registered_taskmanagers, mock.patch(
        "paasta_tools.check_flink_services_health.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        flink_instance_config.config_dict["taskmanager"] = {"instances": 3}
        check_flink_services_health.check_flink_service_health(
            instance_config=flink_instance_config,
            all_tasks_or_pods=all_pods,
            smartstack_replication_checker=None,
        )
        expected = [
            mock.call(
                instance_config=flink_instance_config,
                expected_count=1,
                num_available=1,
                sub_component="supervisor",
            ),
            mock.call(
                instance_config=flink_instance_config,
                expected_count=1,
                num_available=1,
                sub_component="jobmanager",
            ),
            mock.call(
                instance_config=flink_instance_config,
                expected_count=3,
                num_available=1,
                sub_component="taskmanager",
            ),
        ]
        mock_check_under_replication.assert_has_calls(expected)
        mock_check_under_registered_taskmanagers.assert_called_once_with(
            instance_config=flink_instance_config, expected_count=3,
        )
        mock_send_replication_event.assert_called_once_with(
            instance_config=flink_instance_config,
            status=pysensu_yelp.Status.OK,
            output="OK\n########\nOK\n########\nOK\n########\nOK",
        )


def test_check_flink_service_health_too_few_taskmanagers(flink_instance_config):
    def check_under_replication_side_effect(*args, **kwargs):
        if kwargs["sub_component"] == "supervisor":
            return False, "OK"
        if kwargs["sub_component"] == "jobmanager":
            return False, "OK"
        if kwargs["sub_component"] == "taskmanager":
            return True, "NOPE"

    all_pods = []
    with mock.patch(
        "paasta_tools.check_flink_services_health.healthy_flink_containers_cnt",
        autospec=True,
        return_value=1,
    ), mock.patch(
        "paasta_tools.check_flink_services_health.check_under_registered_taskmanagers",
        autospec=True,
        return_value=(True, "NOPE"),
    ) as mock_check_under_registered_taskmanagers, mock.patch(
        "paasta_tools.check_flink_services_health.check_under_replication",
        autospec=True,
        side_effect=check_under_replication_side_effect,
    ) as mock_check_under_replication, mock.patch(
        "paasta_tools.check_flink_services_health.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        flink_instance_config.config_dict["taskmanager"] = {"instances": 3}
        check_flink_services_health.check_flink_service_health(
            instance_config=flink_instance_config,
            all_tasks_or_pods=all_pods,
            smartstack_replication_checker=None,
        )
        expected = [
            mock.call(
                instance_config=flink_instance_config,
                expected_count=1,
                num_available=1,
                sub_component="supervisor",
            ),
            mock.call(
                instance_config=flink_instance_config,
                expected_count=1,
                num_available=1,
                sub_component="jobmanager",
            ),
            mock.call(
                instance_config=flink_instance_config,
                expected_count=3,
                num_available=1,
                sub_component="taskmanager",
            ),
        ]
        mock_check_under_replication.assert_has_calls(expected)
        mock_check_under_registered_taskmanagers.assert_called_once_with(
            instance_config=flink_instance_config, expected_count=3,
        )
        mock_send_replication_event.assert_called_once_with(
            instance_config=flink_instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output="OK\n########\nOK\n########\nNOPE\n########\nNOPE",
        )


def test_check_flink_service_health_under_registered_taskamanagers(
    flink_instance_config,
):
    all_pods = []
    with mock.patch(
        "paasta_tools.check_flink_services_health.healthy_flink_containers_cnt",
        autospec=True,
        return_value=1,
    ), mock.patch(
        "paasta_tools.check_flink_services_health.check_under_replication",
        autospec=True,
        return_value=(False, "OK"),
    ) as mock_check_under_replication, mock.patch(
        "paasta_tools.check_flink_services_health.check_under_registered_taskmanagers",
        autospec=True,
        return_value=(True, "NOPE"),
    ) as mock_check_under_registered_taskmanagers, mock.patch(
        "paasta_tools.check_flink_services_health.send_replication_event", autospec=True
    ) as mock_send_replication_event:
        flink_instance_config.config_dict["taskmanager"] = {"instances": 3}
        check_flink_services_health.check_flink_service_health(
            instance_config=flink_instance_config,
            all_tasks_or_pods=all_pods,
            smartstack_replication_checker=None,
        )
        expected = [
            mock.call(
                instance_config=flink_instance_config,
                expected_count=1,
                num_available=1,
                sub_component="supervisor",
            ),
            mock.call(
                instance_config=flink_instance_config,
                expected_count=1,
                num_available=1,
                sub_component="jobmanager",
            ),
            mock.call(
                instance_config=flink_instance_config,
                expected_count=3,
                num_available=1,
                sub_component="taskmanager",
            ),
        ]
        mock_check_under_replication.assert_has_calls(expected)
        mock_check_under_registered_taskmanagers.assert_called_once_with(
            instance_config=flink_instance_config, expected_count=3,
        )
        mock_send_replication_event.assert_called_once_with(
            instance_config=flink_instance_config,
            status=pysensu_yelp.Status.CRITICAL,
            output="OK\n########\nOK\n########\nOK\n########\nNOPE",
        )
