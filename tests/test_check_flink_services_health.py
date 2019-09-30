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
    mock_instance_config.get_replication_crit_percentage.return_value = 90
    mock_instance_config.get_registrations.return_value = [job_id]
    return mock_instance_config


def test_check_flink_service_health(instance_config):
    all_pods = []
    with mock.patch(
        "paasta_tools.check_flink_services_health.healthy_flink_containers_cnt",
        autospec=True,
        return_value=1,
    ), mock.patch(
        "paasta_tools.monitoring_tools.send_replication_event_if_under_replication",
        autospec=True,
    ) as mock_send_replication_event_if_under_replication:
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
        mock_send_replication_event_if_under_replication.assert_has_calls(expected)
