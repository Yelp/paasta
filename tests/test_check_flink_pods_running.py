import mock
import pysensu_yelp
import pytest

from paasta_tools import check_flink_pods_running
from paasta_tools import check_services_replication_tools
from paasta_tools.check_flink_pods_running import running_flink_pods_cnt
from paasta_tools.check_flink_pods_running import TEAM
from paasta_tools.kubernetes_tools import PodStatus
from paasta_tools.kubernetes_tools import V1Pod

check_flink_pods_running.log = mock.Mock()
check_services_replication_tools.log = mock.Mock()


class TestRunningFlinkPodCnt:
    @pytest.mark.parametrize(
        "pod_status, expected_cnt",
        [pytest.param(PodStatus.RUNNING, 1), pytest.param(PodStatus.PENDING, 0)],
    )
    def test_running_flink_pod(self, pod_status, expected_cnt):
        all_pods = [V1Pod()]
        with mock.patch(
            "paasta_tools.kubernetes_tools.get_pod_status",
            autospec=True,
            return_value=pod_status,
        ):
            assert running_flink_pods_cnt(all_pods) == expected_cnt


class TestCheckFlinkPodRunning:
    @pytest.mark.parametrize(
        "running_cnt, under_replication_return_value, status",
        [
            pytest.param(
                5, (False, "OK"), pysensu_yelp.Status.OK, id="when_all_running"
            ),
            pytest.param(
                4,
                (True, "NOT OK"),
                pysensu_yelp.Status.CRITICAL,
                id="when_not_all_running",
            ),
        ],
    )
    def test_check_flink_pod_running(
        self, flink_instance_config, running_cnt, under_replication_return_value, status
    ):
        all_pods = []
        flink_instance_config.config_dict["taskmanager"] = {"instances": 3}
        with mock.patch(
            "paasta_tools.check_flink_pods_running.running_flink_pods_cnt",
            autospec=True,
            return_value=running_cnt,
        ), mock.patch(
            "paasta_tools.check_flink_pods_running.check_under_replication",
            autospec=True,
            return_value=under_replication_return_value,
        ) as mock_check_under_replication, mock.patch(
            "paasta_tools.check_flink_pods_running.send_replication_event",
            autospec=True,
        ) as mock_send_replication_event:
            check_flink_pods_running.check_flink_pods_running(
                instance_config=flink_instance_config,
                all_tasks_or_pods=all_pods,
                smartstack_replication_checker=None,
            )
            mock_check_under_replication.assert_called_once_with(
                instance_config=flink_instance_config,
                expected_count=5,
                num_available=running_cnt,
            )
            mock_send_replication_event.assert_called_once_with(
                instance_config=flink_instance_config,
                status=status,
                output=under_replication_return_value[1],
                team_override=TEAM,
            )
